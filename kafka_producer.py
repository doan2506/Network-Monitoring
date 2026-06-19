import pandas as pd
import json
import time
import glob
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def run_producer():
    print("Initialize Kafka Producer")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'], # Connecting to Kafka exposed on host
        value_serializer=json_serializer
    )

    topic_name = 'network_traffic'
    parquet_files = ['./data']
    
    if not parquet_files:
        print("No Parquet files found in ./data directory.")
        return

    print(f"Found {len(parquet_files)} parquet files. Starting to stream...")

    for file in parquet_files:
        print(f"Reading file: {file}")
        try:
            df = pd.read_parquet(file)
            drop_cols = ['Label', 'source_file', 'target']
            for col in drop_cols:
                if col in df.columns:
                    df = df.drop(columns=[col])
            
            for index, row in df.iterrows():
                record = row.to_dict()
                producer.send(topic_name, record)
                
                if index > 0 and index % 100 == 0:
                    print(f"Sent {index} records from {file.split('/')[-1]}...")
                
                # Simulate real-time delay (e.g., 50ms per record)
                time.sleep(0.05)
                
        except Exception as e:
            print(f"Error reading or sending data from {file}: {e}")

    producer.flush()
    print("Finished streaming all files.")

if __name__ == "__main__":
    run_producer()
