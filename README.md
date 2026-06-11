# Real-Time Network Monitoring & Incident Detection Pipeline

## Architecture Overview
This project implements a big data pipeline to monitor network traffic in real-time, detecting cyber attacks using the CIC-IDS2017 dataset and a pre-trained Random Forest model.

### Dataset
- **CIC-IDS2017**: [https://www.kaggle.com/datasets/dhoogla/cicids2017](https://www.kaggle.com/datasets/dhoogla/cicids2017)

### Technology Stack
- **Kafka & Zookeeper**: Ingests and buffers real-time network flow data.
- **Spark Structured Streaming**: Processes real-time streams, executes the ML model for attack detection, and handles data sinks.
- **Hadoop (HDFS)**: Long-term storage for historical data and analysis (simulated locally in `./hdfs_data`).
- **InfluxDB**: Time-series database storing aggregated traffic metrics and incident alerts.
- **Grafana**: Visualization dashboard for real-time monitoring of network health and incident detection.

---

## Project Structure
- `docker-compose.yml`: Provisions Zookeeper, Kafka, InfluxDB, and Grafana.
- `kafka_producer.py`: Reads CIC-IDS2017 `.parquet` files and streams them into Kafka topic `network_traffic`.
- `spark_streaming.py`: Spark job that consumes from Kafka, applies the Scikit-learn `rf_pipeline.pkl` model using Pandas UDF, and writes metrics to InfluxDB and raw processed data to HDFS.
- `requirements.txt`: Python dependencies required to run the pipeline.

---

## How to Run

### 1. Start Infrastructure
Launch the cluster using Docker Compose:
```bash
docker-compose up -d
```
This starts:
- Zookeeper (Port `2181`)
- Kafka (Port `9092` & `29092`)
- InfluxDB (Port `8086`)
- Grafana (Port `3000`)

### 2. Install Dependencies
It is highly recommended to create a virtual environment before installing the dependencies.

**Create and activate a virtual environment:**
```bash
# Create a virtual environment named 'venv'
python -m venv venv

# Activate it (Windows)
venv\Scripts\activate
# Activate it (Linux/macOS)
source venv/bin/activate
```

Then, install the requirements:
```bash
pip install -r requirements.txt
```

### 3. Start the Spark Streaming Job
In a new terminal, launch the PySpark streaming process:
```bash
python spark_streaming.py
```
*(Wait until it says "Starting Streaming Query...")*

### 4. Start the Kafka Producer
In another terminal, run the script to simulate network traffic generation:
```bash
python kafka_producer.py
```

### 5. View Real-Time Dashboard (Grafana)
1. Open Grafana at [http://localhost:3000](http://localhost:3000)
2. Login with `admin` / `admin`.
3. Add a Data Source: Choose **InfluxDB**.
    - **URL**: `http://influxdb:8086`
    - **Database**: `ids_db`
4. Create a Dashboard tracking `network_traffic_stats` measurement:
    - **Total Flows**: Monitor `total_flows` field.
    - **Attacks Detected**: Monitor `attack_count` field.
    - **Benign Traffic**: Monitor `benign_count` field.
