# Real-Time Network Monitoring & Incident Detection Pipeline

## Demo & Deliverables
*   **Demo Video**: [Watch E2E System Demo Video on Google Drive](https://drive.google.com/file/d/1TqQe7CIiEVBauEuLulRYxk8pxjPPVIJA/view)
*   **Project Report**: `report.pdf` (LaTeX Source: [report.tex](file:///f:/Tin/code/UETTTTTTTTTTTTTTTTTTTTTTT/Big%20Data/Network-Monitoring/report.tex))
*   **Presentation Slides**: `slide.pdf`

## Architecture Overview
This project implements a big data pipeline to monitor network traffic in real-time, detecting cyber attacks using the CIC-IDS2017 dataset and a native PySpark MLlib Random Forest model.

### Dataset
- **Full CIC-IDS2017**: [https://www.unb.ca/cic/datasets/ids-2017.html](https://www.unb.ca/cic/datasets/ids-2017.html)
  - *Note: For demo purposes, we have included only the `data/parquet/DoS-Wednesday-WorkingHours.pcap_ISCX.parquet` file so you can test the pipeline out-of-the-box with DoS attacks.*
  - *To run a full simulation with all attack types, download the raw `.csv` files from the link above, run the preprocessing notebooks, and place the resulting files in your data directories.*
### Technology Stack
- **Kafka & Zookeeper**: Ingests and buffers real-time network flow data.
- **Spark Structured Streaming**: Processes real-time streams, executes the ML model for attack detection, and handles data sinks.
- **Hadoop (HDFS)**: Long-term storage for historical data and analysis (simulated locally in `./hdfs_data`).
- **InfluxDB**: Time-series database storing aggregated traffic metrics and incident alerts.
- **Grafana**: Visualization dashboard for real-time monitoring of network health and incident detection.

---

## Project Structure
- `docker-compose.yml`: Provisions Zookeeper, Kafka, InfluxDB, and Grafana.
- `kafka_producer.py`: Reads Parquet traffic dataset files and streams them into Kafka topic `network_traffic` with a 50ms delay simulation.
- `spark_streaming.py`: Spark Structured Streaming job that consumes from Kafka, applies the native PySpark MLlib `PipelineModel` (`spark_rf_model`) directly in the JVM for real-time predictions, writes metrics to InfluxDB, saves processed data to local Parquet files, and posts alerts to `network_alerts`.
- `telegram_alerter.py`: A daemon script that consumes from `network_alerts` topic and posts incident summaries to a Telegram channel.
- `smoke_test_model.py`: Isolation test to verify model loading.
- `delete_crc.py`: Helper script to clean up CRC checksum files to prevent Windows/WSL path mismatches.
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

# Activate it (Linux/macOS)
source venv/bin/activate
```

Then, install the requirements:
```bash
pip install -r requirements.txt
```

### 3. Install Java 17 (Required for PySpark)
Apache Spark requires Java to run. Ensure you have **Java 17** installed on your system.

1. Install Java 17:
```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
```
2. Set the `JAVA_HOME` environment variable:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### 4. Start the Spark Streaming Job
In a new terminal, launch the PySpark streaming process:
```bash
python spark_streaming.py
```
*(Wait until it says "Starting Streaming Query...")*

### 5. Start the Kafka Producer
In another terminal, run the script to simulate network traffic generation:
```bash
python kafka_producer.py
```

### 6. View Real-Time Dashboard (Grafana)
1. Open Grafana at [http://localhost:3000](http://localhost:3000)
2. Login with `admin` / `admin`.
3. Add a Data Source: Choose **InfluxDB**.
    - **URL**: `http://influxdb:8086`
    - **Database**: `ids_db`
4. Create a Dashboard tracking `network_traffic_stats` measurement:
    - **Total Flows**: Monitor `total_flows` field.
    - **Attacks Detected**: Monitor `attack_count` field.
    - **Benign Traffic**: Monitor `benign_count` field.
