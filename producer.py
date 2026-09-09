import time
import json
import yfinance as yf
from kafka import KafkaProducer

# Since this script runs on the actual server, it targets localhost directly
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stockmarket"
TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT"]
print(f"[INFO] Initializing Cloud-Native Ingestion Loop via: {KAFKA_BROKER}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
print("[SUCCESS] Pipeline connected! Streaming Yahoo Finance ticks into Kafka...")
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
                print(f"[DATA] Successfully Streamed: {symbol} | Current Price: ${payload['Close']}")
            else:
                print(f"[WARN] Data temporarily missing for {symbol}")
                
            time.sleep(2)
            
except KeyboardInterrupt:
    print("\n[STOP] Ingestion loop terminated cleanly.")
finally:
    producer.close()
