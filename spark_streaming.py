import os
import time
import warnings
import json
import urllib.request
import urllib.parse
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml import PipelineModel
from influxdb import InfluxDBClient
from kafka import KafkaProducer

warnings.filterwarnings("ignore")

# Khởi tạo Kafka Producer dùng cho Alerting
alert_producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Global InfluxDB client reference (explicitly initialized in main)
influx_client = None

# Feature columns extracted from notebook
FEATURE_COLS = [
    'Flow Duration', 'Total Fwd Packets', 'Total Backward Packets', 
    'Fwd Packets Length Total', 'Bwd Packets Length Total', 'Fwd Packet Length Max', 
    'Fwd Packet Length Min', 'Fwd Packet Length Mean', 'Fwd Packet Length Std', 
    'Bwd Packet Length Max', 'Bwd Packet Length Min', 'Bwd Packet Length Mean', 
    'Bwd Packet Length Std', 'Flow Bytes/s', 'Flow Packets/s', 'Flow IAT Mean', 
    'Flow IAT Std', 'Flow IAT Max', 'Flow IAT Min', 'Fwd IAT Total', 'Fwd IAT Mean', 
    'Fwd IAT Std', 'Fwd IAT Max', 'Fwd IAT Min', 'Bwd IAT Total', 'Bwd IAT Mean', 
    'Bwd IAT Std', 'Bwd IAT Max', 'Bwd IAT Min', 'Fwd PSH Flags', 'Bwd PSH Flags', 
    'Fwd URG Flags', 'Bwd URG Flags', 'Fwd Header Length', 'Bwd Header Length', 
    'Fwd Packets/s', 'Bwd Packets/s', 'Packet Length Min', 'Packet Length Max', 
    'Packet Length Mean', 'Packet Length Std', 'Packet Length Variance', 
    'FIN Flag Count', 'SYN Flag Count', 'RST Flag Count', 'PSH Flag Count', 
    'ACK Flag Count', 'URG Flag Count', 'CWE Flag Count', 'ECE Flag Count', 
    'Down/Up Ratio', 'Avg Packet Size', 'Avg Fwd Segment Size', 'Avg Bwd Segment Size', 
    'Fwd Avg Bytes/Bulk', 'Fwd Avg Packets/Bulk', 'Fwd Avg Bulk Rate', 
    'Bwd Avg Bytes/Bulk', 'Bwd Avg Packets/Bulk', 'Bwd Avg Bulk Rate', 
    'Subflow Fwd Packets', 'Subflow Fwd Bytes', 'Subflow Bwd Packets', 
    'Subflow Bwd Bytes', 'Init Fwd Win Bytes', 'Init Bwd Win Bytes', 
    'Fwd Act Data Packets', 'Fwd Seg Size Min', 'Active Mean', 'Active Std', 
    'Active Max', 'Active Min', 'Idle Mean', 'Idle Std', 'Idle Max', 'Idle Min'
]

# Telegram function removed: Handled by external telegram_alerter.py via Kafka

# Create Spark Session
spark = SparkSession.builder \
    .appName("IDS_RealTime_Monitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema for incoming JSON data
schema = StructType([StructField(c, DoubleType(), True) for c in FEATURE_COLS])

# ML model prediction is now handled natively by Spark MLlib PipelineModel loaded at runtime.

def process_batch(batch_df, batch_id):
    start_time = time.time()
    batch_df.write.mode("append").parquet("./hdfs_data/processed_traffic")
    
    # Compute metrics using Spark aggregation to avoid pulling raw data to the driver
    metrics_df = batch_df.select(
        F.count("*").alias("total_flows"),
        F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias("attack_count"),
        F.sum(F.when(F.col("prediction") == 0, 1).otherwise(0)).alias("benign_count"),
        
        # TCP Flags
        F.sum("SYN Flag Count").alias("syn_flags"),
        F.sum("ACK Flag Count").alias("ack_flags"),
        F.sum("RST Flag Count").alias("rst_flags"),
        F.sum("FIN Flag Count").alias("fin_flags"),
        F.sum("PSH Flag Count").alias("psh_flags"),
        
        # Traffic Volume & Rates
        F.avg("Flow Packets/s").alias("flow_packets_s"),
        F.avg("Flow Bytes/s").alias("flow_bytes_s"),
        
        # Packet Lengths
        F.max("Fwd Packet Length Max").alias("fwd_pkt_len_max"),
        F.max("Bwd Packet Length Max").alias("bwd_pkt_len_max"),
        
        # Timing & Duration
        F.avg("Flow Duration").alias("flow_duration")
    )
    
    metrics = metrics_df.collect()[0]
    total_records = metrics["total_flows"]
    
    if total_records == 0:
        return
        
    attack_count = int(metrics["attack_count"]) if metrics["attack_count"] is not None else 0
    benign_count = int(metrics["benign_count"]) if metrics["benign_count"] is not None else 0
        
    json_body = [
        {
            "measurement": "network_traffic_stats",
            "tags": {
                "source": "spark_streaming"
            },
            "fields": {
                "total_flows": total_records,
                "attack_count": attack_count,
                "benign_count": benign_count,
                "syn_flags": int(metrics["syn_flags"]) if metrics["syn_flags"] is not None else 0,
                "ack_flags": int(metrics["ack_flags"]) if metrics["ack_flags"] is not None else 0,
                "rst_flags": int(metrics["rst_flags"]) if metrics["rst_flags"] is not None else 0,
                "fin_flags": int(metrics["fin_flags"]) if metrics["fin_flags"] is not None else 0,
                "psh_flags": int(metrics["psh_flags"]) if metrics["psh_flags"] is not None else 0,
                "flow_packets_s": float(metrics["flow_packets_s"]) if metrics["flow_packets_s"] is not None else 0.0,
                "flow_bytes_s": float(metrics["flow_bytes_s"]) if metrics["flow_bytes_s"] is not None else 0.0,
                "fwd_pkt_len_max": float(metrics["fwd_pkt_len_max"]) if metrics["fwd_pkt_len_max"] is not None else 0.0,
                "bwd_pkt_len_max": float(metrics["bwd_pkt_len_max"]) if metrics["bwd_pkt_len_max"] is not None else 0.0,
                "flow_duration": float(metrics["flow_duration"]) if metrics["flow_duration"] is not None else 0.0
            }
        }
    ]
    try:
        if influx_client is not None:
            influx_client.write_points(json_body)
            
        duration = time.time() - start_time
        throughput = total_records / duration if duration > 0 else 0
        print(f"Batch {batch_id} processed: {total_records} records (Benign: {benign_count}, Attack: {attack_count}) in {duration:.3f} sec ({throughput:.1f} records/sec).")
        
        if attack_count > 0:
            # Gửi toàn bộ metrics của batch này sang Kafka
            alert_payload = {
                "total_flows": total_records,
                "attack_count": attack_count,
                "benign_count": benign_count,
                "syn_flags": int(metrics["syn_flags"]) if metrics["syn_flags"] is not None else 0,
                "ack_flags": int(metrics["ack_flags"]) if metrics["ack_flags"] is not None else 0,
                "rst_flags": int(metrics["rst_flags"]) if metrics["rst_flags"] is not None else 0,
                "fin_flags": int(metrics["fin_flags"]) if metrics["fin_flags"] is not None else 0,
                "psh_flags": int(metrics["psh_flags"]) if metrics["psh_flags"] is not None else 0,
                "flow_packets_s": float(metrics["flow_packets_s"]) if metrics["flow_packets_s"] is not None else 0.0,
                "flow_bytes_s": float(metrics["flow_bytes_s"]) if metrics["flow_bytes_s"] is not None else 0.0,
                "fwd_pkt_len_max": float(metrics["fwd_pkt_len_max"]) if metrics["fwd_pkt_len_max"] is not None else 0.0,
                "bwd_pkt_len_max": float(metrics["bwd_pkt_len_max"]) if metrics["bwd_pkt_len_max"] is not None else 0.0,
                "flow_duration": float(metrics["flow_duration"]) if metrics["flow_duration"] is not None else 0.0
            }
            alert_producer.send('network_alerts', alert_payload)
            alert_producer.flush()
            
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

def main():
    global influx_client
    print("Connecting to InfluxDB...")
    try:
        influx_client = InfluxDBClient(host='localhost', port=8086, database='ids_db')
        influx_client.create_database('ids_db')
    except Exception as e:
        print(f"Warning: Could not initialize InfluxDB database: {e}")
        
    print("Loading PySpark MLlib PipelineModel...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(script_dir, "training_model", "spark_rf_model")
    try:
        model = PipelineModel.load(model_path)
        print("Model loaded successfully.")
    except Exception as e:
        print(f"Error loading PipelineModel from {model_path}: {e}")
        return
        
    print("Connecting to Kafka...")
    
    # Read Stream from Kafka
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "network_traffic") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON values
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    df_clean = df_parsed.na.fill(0.0)

    # Assert schema compatibility before predicting
    missing_cols = set(FEATURE_COLS) - set(df_clean.columns)
    assert not missing_cols, f"Missing feature columns in the input stream: {missing_cols}"

    # Predict using native PySpark MLlib PipelineModel
    df_predictions = model.transform(df_clean)

    # Sink: Write to InfluxDB, HDFS and Kafka Alerts via foreachBatch
    print("Starting Streaming Query...")
    query = df_predictions.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints/ids_monitor") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
