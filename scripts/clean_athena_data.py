import time
import os
from pathlib import Path
import boto3
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

# --- 2. Security Extraction & Cleaning ---
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip("'\" ")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip("'\" ")
aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip("'\" ")

print(f"[DIAGNOSTIC] Current loaded Access Key starts with: {aws_access_key[:5]}...")
print(f"[DIAGNOSTIC] Current targeted AWS Region is: {aws_region}")

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

# --- 4. Infrastructure Context Configurations ---
DATABASE_NAME = "stock_market_db"
RAW_TABLE = "raw_stock_data"
CLEANED_TABLE = "cleaned_stock_data"

# Data lake pointer tracks to hold streaming files
S3_DATA_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/raw_stock_data/"
S3_CLEANED_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/cleaned_stock_data/"
S3_OUTPUT_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/athena_results/"

# --- 5. Query Engineering Statements ---
# Ensure database namespace is active
create_db_query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"

# Explicit raw metadata map fallback using varchar instead of string to prevent Hive engine mismatches
create_raw_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.{RAW_TABLE} (
    `Index` varchar(255),
    `Timestamp` bigint,
    `Open` double,
    `High` double,
    `Low` double,
    `Close` double,
    `Volume` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{S3_DATA_LOCATION}';
"""

# SQL Query to drop the clean table if you are re-running the cleaning pass
drop_table_query = f"DROP TABLE IF EXISTS {DATABASE_NAME}.{CLEANED_TABLE};"

# CTAS Query: Cleans, filters, transforms, and stores data as optimized JSON back to S3
clean_data_query = f"""
CREATE TABLE {DATABASE_NAME}.{CLEANED_TABLE}
WITH (
    format = 'JSON',
    external_location = '{S3_CLEANED_LOCATION}'
) AS 
SELECT 
    CAST("index" AS varchar) as index,
    CAST("timestamp" AS bigint) as timestamp,
    CAST("open" AS double) as open,
    CAST("high" AS double) as high,
    CAST("low" AS double) as low,
    CAST("close" AS double) as close,
    CAST("volume" AS bigint) as volume
FROM {DATABASE_NAME}.{RAW_TABLE}
WHERE "timestamp" IS NOT NULL 
  AND "timestamp" > 0
  AND "open" IS NOT NULL 
  AND "high" IS NOT NULL 
  AND "low" IS NOT NULL 
  AND "close" IS NOT NULL
  AND "volume" >= 0;
"""

def run_athena_cleaning(query_string, context_message):
    print(f"[EXECUTING] {context_message}...")
    
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
        print("⏳ Athena is processing data catalog requests...")
        time.sleep(2)
        
    if state == 'SUCCEEDED':
        print(f"[SUCCESS] {context_message} complete. Query ID: {query_id}")
    else:
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"❌ Query failed in state {state}. Reason: {reason}")

try:
    # Step 1: Ensure database container exists
    run_athena_cleaning(create_db_query, f"Verifying Database Namespace ({DATABASE_NAME})")
    
    # Step 2: Auto-inject Schema Map straight onto your raw S3 storage track files
    run_athena_cleaning(create_raw_table_query, f"Re-asserting Schema Layout on Raw Logs Table ({RAW_TABLE})")
    
    # Step 3: Drop old clean table references if they exist
    run_athena_cleaning(drop_table_query, "Clearing out old clean metadata layout links")
    
    # Step 4: Run data cleaning and write the clean dataset to S3
    run_athena_cleaning(clean_data_query, "Filtering out nulls/bad records and generating Cleaned Table")
    
    print(f"\n🎉 Clean up complete! Your pristine dataset is now physically stored in S3 at:\n👉 {S3_CLEANED_LOCATION}")

except Exception as e:
    print(f"\n[ERROR] Data cleaning task failed: {str(e)}")
