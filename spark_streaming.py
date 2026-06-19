import os
import joblib
import pandas as pd
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, pandas_udf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from influxdb import InfluxDBClient

# Feature columns extracted from notebook
FEATURE_COLS = [
    'Protocol', 'Flow Duration', 'Total Fwd Packets', 'Total Backward Packets', 
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

# Create Spark Session
spark = SparkSession.builder \
    .appName("IDS_RealTime_Monitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema for incoming JSON data
schema = StructType([StructField(c, DoubleType(), True) for c in FEATURE_COLS])

# Broadcast Scikit-learn Pipeline
model_path = os.path.abspath("./training_model/rf_pipeline.pkl")
rf_pipeline = joblib.load(model_path)
broadcast_model = spark.sparkContext.broadcast(rf_pipeline)

# Pandas UDF for model inference
@pandas_udf(IntegerType())
def predict_attack(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    model = broadcast_model.value
    for pdf in iterator:
        pdf = pdf[FEATURE_COLS]
        pdf = pdf.fillna(0.0)
        predictions = model.predict(pdf)
        yield pd.Series(predictions)

def process_batch(batch_df, batch_id):
    """
    ForeachBatch function to write predictions to InfluxDB and HDFS
    """
    # Simulate writing to Hadoop/HDFS by saving locally
    batch_df.write.mode("append").parquet("./hdfs_data/processed_traffic")
    
    # Compute metrics using Spark aggregation to avoid pulling raw data to the driver
    metrics_df = batch_df.select(
        F.count("*").alias("total_flows"),
        F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias("attack_count"),
        F.sum(F.when(F.col("prediction") == 0, 1).otherwise(0)).alias("benign_count")
    )
    
    metrics = metrics_df.collect()[0]
    total_records = metrics["total_flows"]
    
    if total_records == 0:
        return
        
    attack_count = int(metrics["attack_count"]) if metrics["attack_count"] is not None else 0
    benign_count = int(metrics["benign_count"]) if metrics["benign_count"] is not None else 0
        
    client = InfluxDBClient(host='localhost', port=8086, database='ids_db')
    
    json_body = [
        {
            "measurement": "network_traffic_stats",
            "tags": {
                "source": "spark_streaming"
            },
            "fields": {
                "total_flows": total_records,
                "attack_count": attack_count,
                "benign_count": benign_count
            }
        }
    ]
    try:
        # Need to create DB if not exists
        client.create_database('ids_db')
        client.write_points(json_body)
        print(f"Batch {batch_id} processed: {total_records} records, {attack_count} attacks detected.")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

def main():
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

    # Predict using ML Model UDF
    features_struct = struct(*[col(c) for c in FEATURE_COLS])
    df_predictions = df_clean.withColumn("prediction", predict_attack(features_struct))

    # Sink: Write to InfluxDB and HDFS
    print("Starting Streaming Query...")
    query = df_predictions.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints/ids_monitor") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
