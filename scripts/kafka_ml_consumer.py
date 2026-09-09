import json
import os
import collections
import time
import warnings
from pathlib import Path
import boto3
import pandas as pd
import joblib
from kafka import KafkaConsumer
from dotenv import load_dotenv

# --- 1. Dynamic Path Resolution for Project Environment ---
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
env_path = root_dir / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"[INFO] Loaded environment configuration from: {env_path}")

# --- 2. Security Extraction ---
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip("'\" ")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip("'\" ")
aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip("'\" ")

if not aws_access_key or not aws_secret_key:
    raise ValueError("❌ Critical Error: AWS keys are missing inside your .env file!")

# --- 3. ML Model Loading & AWS Initialization ---
FEATURE_NAMES = ["price_lag_1", "price_lag_2", "price_lag_3"]
MODEL_PATH = root_dir / "models" / "stock_predictor_model.pkl"
S3_BUCKET_NAME = "kafka-stockmarket-data-lake-lenovo"
S3_ENRICHED_PREFIX = "enriched_stock_data"

print(f"[INFO] Loading predictive ML brain from: {MODEL_PATH}...")
model = joblib.load(MODEL_PATH)

print("[INFO] Creating explicit AWS S3 session context...")
session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
s3_client = session.client('s3')

# --- 4. MULTI-ASSET SLIDING WINDOW MEMORY DICTIONARY ---
ticker_buffers = {}

# --- 5. Kafka Infrastructure Configurations ---
KAFKA_EC2_PUBLIC_IP = "44.211.164.195" 
KAFKA_PORT = "9092"
TOPIC_NAME = "stockmarket"

print(f"[INFO] Connecting to Live Kafka Broker at {KAFKA_EC2_PUBLIC_IP}:{KAFKA_PORT}...")
try:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[f"{KAFKA_EC2_PUBLIC_IP}:{KAFKA_PORT}"],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
    print(f"📡 Successfully connected to Kafka topic '{TOPIC_NAME}'. Awaiting live multi-stock stream...")
except Exception as kafka_err:
    print(f"❌ Kafka Connection failed: {str(kafka_err)}")
    exit(1)

# --- 6. Live Streaming Processing & Real-Time ML Inference Loop ---
try:
    for message in consumer:
        tick_data = message.value
        
        current_close = float(tick_data.get("Close", tick_data.get("close", 0)))
        timestamp = str(tick_data.get("Timestamp", tick_data.get("timestamp", int(time.time()))))
        index_label = str(tick_data.get("Index", tick_data.get("index", "UNKNOWN"))).strip().upper()
        
        if index_label == "UNKNOWN":
            continue

        if index_label not in ticker_buffers:
            ticker_buffers[index_label] = collections.deque(maxlen=3)
            
        ticker_buffers[index_label].append(current_close)
        current_buffer = ticker_buffers[index_label]
        
        # Only predict if this specific stock has accumulated 3 minutes of data points
        if len(current_buffer) == 3:
            # 💡 FIX: Extract scalar numeric array offsets cleanly to prevent shape/type errors
            lag_1 = float(current_buffer[2])  # t-1 minute (newest)
            lag_2 = float(current_buffer[1])  # t-2 minutes
            lag_3 = float(current_buffer[0])  # t-3 minutes (oldest)
            
            features_df = pd.DataFrame([[lag_1, lag_2, lag_3]], columns=FEATURE_NAMES)
            
            # Execute instant machine learning prediction in local RAM
            prediction_array = model.predict(features_df)
            predicted_price = float(prediction_array[0])
            
            price_delta = predicted_price - current_close
            signal = "BUY" if price_delta > 0.15 else "SELL" if price_delta < -0.15 else "HOLD"
                
            # Maintain exact structural key formatting matching your original app.py expectations
            tick_data["Index"] = index_label
            tick_data["Close"] = current_close
            tick_data["Predicted_Close"] = round(predicted_price, 2)
            tick_data["ML_Signal"] = signal
            tick_data["Pipeline_Status"] = "ENRICHED_WITH_ML"
            
            print(f"📈 [{index_label}] Live Close: ${current_close:.2f} | Next-Min Forecast: ${predicted_price:.2f} | Action: {signal}")
            
            s3_safe_timestamp = timestamp.replace(":", "-").replace(" ", "_")
            s3_key = f"{S3_ENRICHED_PREFIX}/{index_label}/enriched_tick_{s3_safe_timestamp}.json"
            
            try:
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(tick_data))
            except Exception as s3_write_err:
                print(f"⚠️ S3 Pipeline Enrichment write error for [{index_label}]: {str(s3_write_err)}")
                
        else:
            print(f"⏳ Warming up live window buffer for [{index_label}]... ({len(current_buffer)}/3 stock ticks collected)")

except KeyboardInterrupt:
    print("\n🛑 Live Inference Consumer process gracefully stopped by user.")
