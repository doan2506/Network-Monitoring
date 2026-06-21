# Real-Time Network Monitoring & Incident Detection Pipeline

This project implements a big data pipeline to monitor network traffic in real-time, detecting cyber attacks using the **CIC-IDS2017** dataset and a pre-trained Machine Learning (Random Forest) model.

## Dataset
- **Full CIC-IDS2017**: [https://www.unb.ca/cic/datasets/ids-2017.html](https://www.unb.ca/cic/datasets/ids-2017.html)
  - *Note: For demo purposes, we have included only the `data/parquet/DoS-Wednesday-WorkingHours.pcap_ISCX.parquet` file so you can test the pipeline out-of-the-box with DoS attacks.*
  - *To run a full simulation with all attack types, download the raw `.csv` files from the link above, run the preprocessing notebooks, and place the resulting files in your data directories.*

## Architecture Overview

Data processing flow of the system:

```mermaid
flowchart TD
    A[Network Data <br/> CIC-IDS2017 Parquet] -->|kafka_producer.py| B(Kafka Topic: network_traffic)
    
    B -->|Read Stream| C{Apache Spark <br/> Structured Streaming}
    
    subgraph Spark Processing
    C -->|Predict via| M[Spark MLlib Pipeline Model]
    M -->|Process & Aggregate| Agg[Calculate Metrics]
    end
    
    Agg -->|Store Raw Data| D[(Local HDFS <br/> ./hdfs_data)]
    Agg -->|Store Metrics| E[(InfluxDB)]
    Agg -->|If attack detected| F(Kafka Topic: network_alerts)
    
    F -->|Read Alerts| G[telegram_alerter.py]
    G -->|Send Message| H[Telegram Bot API]
    
    E -->|Query Data| I[Grafana Dashboard]
```

### Key Components:
1. **Producer (`kafka_producer.py`)**: Reads data from Parquet files (CIC-IDS2017) and pushes it to Kafka to simulate real-time network traffic.
2. **Streaming Processor (`spark_streaming.py`)**: Consumes data from Kafka and applies a machine learning model (Spark MLlib) to predict whether the traffic is Benign or Attack.
3. **Storage & Metrics**: 
   - Raw processed data is saved to a Local HDFS directory (`./hdfs_data/processed_traffic`).
   - Aggregated metrics are written to the time-series database InfluxDB.
   - If an attack is detected, the metrics are pushed to the `network_alerts` Kafka topic.
4. **Alerting (`telegram_alerter.py`)**: Listens to the `network_alerts` topic and automatically sends real-time alert notifications via Telegram.
5. **Visualization**: **Grafana** pulls data from InfluxDB to display a visual dashboard for monitoring network health.

## Technology Stack

- **Language**: Python
- **Message Broker**: Apache Kafka, Zookeeper
- **Stream Processing**: Apache Spark (Structured Streaming, PySpark)
- **Machine Learning**: Spark MLlib (Random Forest)
- **Data Storage**: 
  - Hadoop/HDFS (Simulated locally)
  - InfluxDB (Time-series Database)
- **Visualization**: Grafana
- **Alerting**: Telegram Bot API
- **Deployment**: Docker Compose

## How to run (Setup & Demo)

### Step 1: Prerequisites
- Install Docker and Docker Compose.
- Install Java 17 (Required to run Apache Spark).
- Install Python 3.8+.

Installing Java 17 (Ubuntu/Linux):
```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### Step 2: Start the Infrastructure
Launch Zookeeper, Kafka, InfluxDB, and Grafana using Docker Compose:
```bash
docker-compose up -d
```

### Step 3: Configure the Python Environment
Create a virtual environment and install the required libraries:
```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/macOS)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 4: Configure the Telegram Bot
To receive alerts via Telegram, create a `.env` file in the root directory (alongside `telegram_alerter.py`) with the following content:
```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here
```

### Step 5: Run the Pipeline Components (Open 3 separate terminals)

**Terminal 1: Start Spark Streaming**
This is the core of the system. Run the command and wait until you see `Starting streaming query`.
```bash
python spark_streaming.py
```

**Terminal 2: Start Telegram Alerter**
Listens for and sends alerts when an attack is detected.
```bash
python telegram_alerter.py
```

**Terminal 3: Start Kafka Producer**
Starts pushing network traffic data into Kafka.
```bash
python kafka_producer.py
```

### Step 6: View the Dashboard on Grafana
1. Open your browser and go to [http://localhost:3000](http://localhost:3000)
2. Login with credentials: `admin` / `admin`.
3. Navigate to **Dashboards** to view the pre-configured Network Monitoring dashboard (Data source and dashboard are automatically provisioned).
