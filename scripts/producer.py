import json
import time
from kafka import KafkaProducer
import yfinance as yf

# --- 1. Kafka Infrastructure Configurations ---
# Pointing directly to your active EC2 Instance running the Kafka Broker
KAFKA_EC2_PUBLIC_IP = "44.211.164.195"
KAFKA_PORT = "9092"
TOPIC_NAME = "stock-ticks"

# The target stock ticker symbol (e.g., Apple Inc. - AAPL, or Index like ^GSPC)
STOCK_SYMBOL = "AAPL" 

print(f"[INFO] Initializing Kafka Producer targeting Broker at {KAFKA_EC2_PUBLIC_IP}:{KAFKA_PORT}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[f"{KAFKA_EC2_PUBLIC_IP}:{KAFKA_PORT}"],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',  # Strongest delivery guarantee
        retries=5
    )
    print(f"📡 Successfully linked to Kafka cluster! Monitoring ticker: {STOCK_SYMBOL}...")
except Exception as e:
    print(f"❌ Failed to connect to Kafka Broker: {str(e)}")
    print("💡 Troubleshooting Tip: Ensure Apache Kafka and Zookeeper are actively running on your EC2 instance and port 9092 is open in its security groups.")
    exit(1)

# --- 2. Live Ingestion & Streaming Loop ---
try:
    print(f"🚀 Starting live market stream loop for {STOCK_SYMBOL}...")
    while True:
        # Fetch the absolute latest market quote from Yahoo Finance
        ticker = yf.Ticker(STOCK_SYMBOL)
        todays_data = ticker.history(period='1d', interval='1m')
        
        if not todays_data.empty:
            # Grab the last complete 1-minute interval record frame
            latest_tick = todays_data.iloc[-1]
            
            # Map the exact schema formatting required by your Athena/ML layout
            payload = {
                "Index": STOCK_SYMBOL,
                "Timestamp": int(time.time()),
                "Open": float(latest_tick['Open']),
                "High": float(latest_tick['High']),
                "Low": float(latest_tick['Low']),
                "Close": float(latest_tick['Close']),
                "Volume": int(latest_tick['Volume'])
            }
            
            # Push payload onto the active Kafka topic stream
            producer.send(TOPIC_NAME, value=payload)
            producer.flush() # Forces network buffer commit
            
            print(f"⚡ [PRODUCER] Streamed Tick -> Close: ${payload['Close']:.2f} | Vol: {payload['Volume']}")
        else:
            print("⏳ Awaiting data frames from the financial API market feed...")
            
        # Stream once every minute (60 seconds) to match real-time interval targets
        time.sleep(60)

except KeyboardInterrupt:
    print("\n🛑 Stock market live streaming producer gracefully stopped by user.")
