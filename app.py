import sys
import asyncio
import os
import json
import boto3
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

# --- 1. Windows Asyncio Bug Fix ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- 2. Robust Environment & Credentials Loading ---
script_dir = Path(__file__).resolve().parent
env_path_current = script_dir / '.env'
env_path_parent = script_dir.parent / '.env'

if env_path_current.exists():
    load_dotenv(dotenv_path=env_path_current)
elif env_path_parent.exists():
    load_dotenv(dotenv_path=env_path_parent)
else:
    load_dotenv()

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip("'\" ")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip("'\" ")
aws_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip("'\" ")

# --- 3. Streamlit Page Setup ---
st.set_page_config(page_title="Multi-Asset Live ML Stream", layout="wide")
st.title("🧠 Real-Time Multi-Asset ML Inference Dashboard")

# --- 4. Sidebar Credential Overrides & Fallbacks ---
st.sidebar.header("🔐 AWS Configuration Status")

if not aws_access_key or not aws_secret_key:
    st.sidebar.warning("⚠️ No keys found in .env file.")
    aws_access_key = st.sidebar.text_input("Enter AWS Access Key ID:", type="password")
    aws_secret_key = st.sidebar.text_input("Enter AWS Secret Access Key:", type="password")
    aws_region = st.sidebar.text_input("Enter AWS Region:", value="us-east-1")
    
    if not aws_access_key or not aws_secret_key:
        st.error("❌ Critical Error: AWS credentials missing! Please configure your local `.env` file or paste them in the sidebar menu.")
        st.stop()
else:
    st.sidebar.success("✅ Credentials loaded safely via environment variables.")

# Initialize AWS Session and S3 Client Context
try:
    session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
    s3_client = session.client('s3')
except Exception as init_err:
    st.error(f"❌ Failed to initialize AWS session: {str(init_err)}")
    st.stop()

S3_BUCKET_NAME = "kafka-stockmarket-data-lake-lenovo"
S3_ENRICHED_PREFIX = "enriched_stock_data"
TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT"]

# --- 5. High-Performance Multi-Asset Fetcher ---
@st.cache_data(ttl=10)
def fetch_multi_asset_data(ticker_list, max_files_per_ticker=50):
    all_records = []
    
    for ticker in ticker_list:
        prefix = f"{S3_ENRICHED_PREFIX}/{ticker}/"
        try:
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
            
            if 'Contents' in response:
                sorted_contents = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
                
                for obj in sorted_contents[:max_files_per_ticker]:
                    if obj['Key'] == prefix:
                        continue
                    
                    file_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
                    file_content = file_obj['Body'].read().decode('utf-8')
                    
                    data_record = json.loads(file_content)
                    all_records.append(data_record)
        except Exception as e:
            st.warning(f"⚠️ Could not pull streaming history for {ticker}: {str(e)}")
            
    return pd.DataFrame(all_records)

# --- 6. Application Execution & UI Rendering ---
with st.spinner("Fetching latest enriched stock records from S3..."):
    df = fetch_multi_asset_data(TICKERS)

if df.empty:
    st.info("⏳ Awaiting streaming data... Confirm that your ML Inference consumer loop is currently processing active tickers.")
else:
    # Enforce uniform lowercase columns to prevent case matching errors
    df.columns = [c.lower() for c in df.columns]
    
    if 'timestamp' in df.columns:
        df['readable_time'] = pd.to_datetime(df['timestamp'], unit='s')
        
    # Ensure all stock strings are cleaned and uppercase
    if 'index' in df.columns:
        df['index'] = df['index'].astype(str).str.upper().str.strip()

    selected_ticker = st.selectbox("🎯 Target Ticker Environment Select:", ["ALL Active Stocks"] + TICKERS)
    
    # Robust filtering matching the cleaned uppercase indices
    if selected_ticker != "ALL Active Stocks":
        filtered_df = df[df['index'] == selected_ticker.upper()]
    else:
        filtered_df = df

    st.subheader(f"📊 Live Signal Intelligence Matrix ({selected_ticker})")
    m_col1, m_col2, m_col3 = st.columns(3)
    with m_col1:
        st.metric(label="Buffered Records Count", value=len(filtered_df))
    with m_col2:
        buy_signals = len(filtered_df[filtered_df['ml_signal'].str.upper() == 'BUY']) if 'ml_signal' in filtered_df.columns else 0
        st.metric(label="Active BUY Signals Detected", value=buy_signals)
    with m_col3:
        sell_signals = len(filtered_df[filtered_df['ml_signal'].str.upper() == 'SELL']) if 'ml_signal' in filtered_df.columns else 0
        st.metric(label="Active SELL Signals Detected", value=sell_signals)

    st.subheader("📈 Real-Time Price vs. Next-Minute ML Prediction Trend")
    if not filtered_df.empty and 'readable_time' in filtered_df.columns:
        if selected_ticker == "ALL Active Stocks":
            chart_data = filtered_df.pivot_table(index='readable_time', columns='index', values='close', aggfunc='last')
            st.line_chart(chart_data)
        else:
            chart_data = filtered_df.sort_values('readable_time')[['readable_time', 'close', 'predicted_close']]
            st.line_chart(chart_data.set_index('readable_time'))

    st.subheader("📋 Enriched Ingestion Ledger Logs")
    display_cols = [c for c in ['index', 'readable_time', 'close', 'predicted_close', 'ml_signal', 'volume'] if c in filtered_df.columns]
    st.dataframe(filtered_df.sort_values(by='timestamp', ascending=False)[display_cols], width="stretch")
