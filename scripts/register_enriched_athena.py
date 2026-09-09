import time
import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

# --- 1. Environment and Path Resolution ---
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
env_path = root_dir / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    print(f"[INFO] Loaded environment configuration from: {env_path}")

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip("'\" ")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip("'\" ")
aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip("'\" ")

# --- 2. Boto3 Session Object Initialization ---
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)
athena_client = session.client('athena')

# --- 3. Configuration & SQL Query Engineering ---
DATABASE_NAME = "stock_market_db"
ENRICHED_TABLE = "enriched_stock_data"

# The targets for where the consumer saves predictions and where Athena logs run
S3_ENRICHED_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/enriched_stock_data/"
S3_OUTPUT_LOCATION = "s3://kafka-stockmarket-data-lake-lenovo/athena_results/"

drop_table_query = f"DROP TABLE IF EXISTS {DATABASE_NAME}.{ENRICHED_TABLE};"

# Maps a schema that matches your real-time enriched consumer's JSON payload additions
create_enriched_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.{ENRICHED_TABLE} (
    `Index` varchar(255),
    `Timestamp` bigint,
    `Open` double,
    `High` double,
    `Low` double,
    `Close` double,
    `Volume` bigint,
    `Predicted_Close` double,
    `ML_Signal` varchar(50),
    `Pipeline_Status` varchar(100)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '{S3_ENRICHED_LOCATION}';
"""

def execute_athena_schema(query_string, context_message):
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
        time.sleep(1)
        
    if state == 'SUCCEEDED':
        print(f"[SUCCESS] {context_message} complete.")
    else:
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"❌ Athena DDL registration failed: {reason}")

try:
    execute_athena_schema(drop_table_query, "Clearing out old enriched schema profiles")
    execute_athena_schema(create_enriched_table_query, f"Mapping ML Enriched Schema onto Table ({ENRICHED_TABLE})")
    print("\n🎉 Project Architecture Complete! Your real-time ML predictions are now searchable via SQL in Amazon Athena.")

except Exception as e:
    print(f"\n[ERROR] Athena schema execution failed: {str(e)}")
