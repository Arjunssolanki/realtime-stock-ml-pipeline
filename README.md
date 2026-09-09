# 📈 Real-Time Multi-Asset Stock Ingestion Pipeline & Streamlit ML Dashboard
### *End-to-End Streaming Architecture (AWS EC2 ──► Apache Kafka ──► Boto3 ──► Amazon S3 ──► Streamlit UI)*

A light, cloud-native data engineering pipeline that ingests live multi-asset stock data via **Apache Kafka** on **AWS EC2**, executes real-time **Machine Learning rolling inference**, pushes structured records to an **AWS S3 Data Lake**, and serves instant predictive insights on a custom-built **Streamlit Web Application**.

---

## 🏗️ System Architecture Flow

```

---

## 🖥️ Infrastructure & Setup

### 1. Cloud & Hardware Spec
* **Instance:** AWS EC2 `t2.micro` (1 GB RAM / 1 vCPU) | Amazon Linux 2023
* **Broker Endpoint:** `44.211.164.195:9092` (Topic: `stockmarket`)
* **Security Group:** Ports `22` (SSH) and `9092` (Kafka) open to `0.0.0.0/0`
* **Local Project Root:** `D:\2026\newstudy\projects\stockmarket`

### 2. Environment Variables (`.env`)
Place this file in your root folder:
```text
AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_ACCESS_KEY_ID"
AWS_DEFAULT_REGION="us-east-1"
```

### 3. Dependencies (`requirements.txt`)
```text
kafka-python
yfinance
pandas
boto3
python-dotenv
joblib
streamlit
```

---

## 🏃‍♂️ Execution Blueprint

Open **four separate terminals** and launch the components in this sequence:

1. **Terminal 1 (Data Ingestion):** Streams 1-minute iterations for **AAPL, GOOGL, AMZN, MSFT**.
   ```bash
   python scripts/producer.py
   ```
2. **Terminal 2 (ML Stream Processing):** Runs streaming inference and dumps JSON files to S3.
   ```bash
   python scripts/kafka_ml_consumer.py
   ```
3. **Terminal 3 (Database Catalog Sync):** Syncs data formats automatically into Glue & Athena.
   ```bash
   python scripts/register_enriched_athena.py
   ```
4. **Terminal 4 (Presentation UI):** Launches the interactive Streamlit web dashboard.
   ```bash
   streamlit run app.py
   ```

---

## 🚀 Completed Milestones

* **Multi-Company Ingestion:** Expanded `producer.py` to seamlessly fetch and stream all 4 stock targets sequentially without blocking.
* **Streamlit Application Deployment:** Successfully engineered a real-time web interface (`app.py`) that monitors data lakes securely using automated `.env` fallback parameters and displays buy/sell metrics instantly.
* **Freeze & Throttling Fix:** Set a constraint threshold (`max_files_per_ticker=50`) inside the Streamlit engine caching state, completely resolving `WinError 10054` network drops caused by S3 request overloading.
* **Serverless SQL Sync:** Deployed `register_enriched_athena.py` to handle structural table creation, metadata layout tracking, and automated partition updates for instant Athena SQL queries.
* **Case-Insensitive UI Filtering:** Forced strict dataframe casing normalization (`.lower()` and `.str.upper()`) across the dashboard tracking arrays so graphs refresh instantly when changing ticker selections.
* **Windows Socket Standardization:** Patched the runtime layer with platform policy intercepts (`asyncio.WindowsSelectorEventLoopPolicy()`) to stop noisy background WebSocket crashes on Windows devices.
