import time
import json
import yfinance as yf
from kafka import KafkaProducer

# --- Configurations ---
KAFKA_BROKER = "44.211.164.195:9092"
TOPIC_NAME = "stockmarket"
TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT"]

print(f"[INFO] Initializing Kafka Producer targeting Broker at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print(f"🔌 Successfully linked to Kafka cluster! Monitoring tickers: {', '.join(TICKERS)}...")
except Exception as e:
    print(f"❌ Failed to connect to Kafka Broker: {e}")
    exit(1)

print("🚀 Starting live multi-stock market stream loop...")
try:
    while True:
        for symbol in TICKERS:
            ticker_obj = yf.Ticker(symbol)
            historical_df = ticker_obj.history(period="1d", interval="1m")
            
            if not historical_df.empty:
                latest_row = historical_df.iloc[-1]
                payload = {
                    "Index": symbol,
                    "Timestamp": int(time.time()),
                    "Open": round(float(latest_row['Open']), 2),
                    "High": round(float(latest_row['High']), 2),
                    "Low": round(float(latest_row['Low']), 2),
                    "Close": round(float(latest_row['Close']), 2),
                    "Volume": int(latest_row['Volume'])
                }
                
                producer.send(TOPIC_NAME, value=payload)
                producer.flush()
                print(f"⚡ [PRODUCER] Streamed Tick -> {symbol} | Close: ${payload['Close']} | Vol: {payload['Volume']}")
            else:
                print(f"[WARN] Data temporarily missing for {symbol}")
                
        # 2-second delay between ticks
        time.sleep(2)
            
except KeyboardInterrupt:
    print("\n🛑 Stock market live streaming producer gracefully stopped by user.")
finally:
    producer.close()
