import time
import json
from kafka import KafkaConsumer
import boto3

# --- 1. Configuration Constants ---
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stockmarket"
S3_BUCKET_NAME = "kafka-stockmarket-data-lake-lenovo" 
print(f"[INFO] Connecting to Kafka Broker via: {KAFKA_BROKER}...")
# Initialize Kafka Consumer to read live streams
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# Initialize programmatic AWS S3 Client using default system credentials
s3_client = boto3.client('s3')
print(f"[SUCCESS] Consumer active! Ready to sink data stream into S3 Bucket: {S3_BUCKET_NAME}...")
try:
    # Continuously grab incoming messages from the Kafka topic
    for count, message in enumerate(consumer):
        payload_data = message.value
        
        # Generate a distinct structural filename using unique timestamps
        file_name = f"stock_record_{int(time.time())}_{count}.json"
        
        print(f"[FETCHED] Processing Row for: {payload_data.get('Index')} | Price: ${payload_data.get('Close')}")
        
        # Stream the raw JSON record directly onto your S3 Storage bucket path
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"raw_stock_data/{file_name}",
            Body=json.dumps(payload_data)
        )      
        print(f"[UPLOADED] Successfully pushed {file_name} into S3 bucket storage.")
except KeyboardInterrupt:
    print("\n[STOP] S3 data consumer loop stopped cleanly by user.")
