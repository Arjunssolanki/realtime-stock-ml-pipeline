import time
import os
from pathlib import Path
import boto3
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
from dotenv import load_dotenv

# --- 1. Dynamic Path Resolution for Project Environment ---
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
env_path = root_dir / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"[INFO] Loaded environment configuration from: {env_path}")
else:
    print(f"[WARN] Active environment configuration file not found at: {env_path}")

# --- 2. Security Extraction ---
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip("'\" ")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip("'\" ")
aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip("'\" ")

if not aws_access_key or not aws_secret_key:
    raise ValueError("❌ Critical Error: AWS keys are missing inside your .env file!")

# --- 3. Boto3 Session Object Initialization ---
print("[INFO] Creating explicit AWS authentication session context...")
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

athena_client = session.client('athena')
s3_client = session.client('s3')

# --- 4. Infrastructure Context Configurations ---
DATABASE_NAME = "stock_market_db"
TABLE_NAME = "cleaned_stock_data"
S3_OUTPUT_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/athena_results/"
MODEL_OUTPUT_PATH = root_dir / "models" / "stock_predictor_model.pkl"

# SQL Query to pull your clean data ordered chronologically by timestamp
select_clean_data_query = f"""
SELECT timestamp, open, high, low, close, volume 
FROM {DATABASE_NAME}.{TABLE_NAME}
ORDER BY timestamp ASC;
"""

def fetch_data_from_athena(query_string):
    print("[EXECUTING] Querying clean records out of Athena database catalog...")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION},
        WorkGroup='primary'
    )
    query_id = response['QueryExecutionId']
    
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        print("⏳ Athena is compiling your ML dataset...")
        time.sleep(2)
        
    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"❌ Athena dataset generation failed: {reason}")
        
    # Get the physical CSV output path compiled by Athena
    output_file_location = status['QueryExecution']['ResultConfiguration']['OutputLocation']
    print(f"[SUCCESS] Query finished. Temporary CSV located at: {output_file_location}")
    
    # Parse S3 URI components to download via boto3
    s3_path_parts = output_file_location.replace("s3://", "").split("/", 1)
    bucket_name = s3_path_parts[0]
    file_key = s3_path_parts[1]
    
    # Read the data straight out of S3 into a standard Pandas DataFrame
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(s3_object['Body'])

try:
    # Step A: Fetch dataset from Athena
    df = fetch_data_from_athena(select_clean_data_query)
    total_records = len(df)
    print(f"📊 Successfully loaded {total_records} rows into machine learning memory.")
    
    if total_records < 5:
        raise ValueError("❌ Not enough data rows to construct time-series lags! Stream more ticks.")

    # Step B: Time-Series Feature Engineering (Sliding Window Lags)
    print("⚙️ Engineering 3-minute lookback features (Lags)...")
    df["price_lag_1"] = df["close"].shift(1)  # t-1 minute
    df["price_lag_2"] = df["close"].shift(2)  # t-2 minutes
    df["price_lag_3"] = df["close"].shift(3)  # t-3 minutes
    
    # Target Variable: Next-minute target price (t+1 minute forward)
    df["target_price"] = df["close"].shift(-1)
    
    # Drop rows at data edges where lagging creates empty NaN structures
    df.dropna(inplace=True)
    
    # Define Input Matrices and Target Vector
    X = df[["price_lag_1", "price_lag_2", "price_lag_3"]]
    y = df["target_price"]
    
    # Step C: Train the Machine Learning Brain
    print("🧠 Training Time-Series Random Forest Regressor Engine...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Step D: Serialize and Export Model Weights
    # Ensure local directory structure exists
    os.makedirs(os.path.dirname(MODEL_OUTPUT_PATH), exist_ok=True)
    joblib.dump(model, MODEL_OUTPUT_PATH)
    
    print(f"\n🎉 SUCCESS! Trained ML brain exported to project tree at:\n👉 {MODEL_OUTPUT_PATH}")

except Exception as e:
    print(f"\n[ERROR] Machine Learning Training Pipeline failed: {str(e)}")
