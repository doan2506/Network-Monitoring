import os
import pandas as pd
import json
import time
import glob
import numpy as np
from kafka import KafkaProducer

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def json_serializer(data):
    return json.dumps(data, cls=NpEncoder).encode('utf-8')

def run_producer():
    print("Initialize Kafka Producer")
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'], # Connecting to Kafka exposed on host
            value_serializer=json_serializer
        )

        topic_name = 'network_traffic'
        parquet_files = glob.glob('./data/**/*.parquet', recursive=True)
        
        if not parquet_files:
            print("No Parquet files found in ./data directory.")
            return

        print(f"Found {len(parquet_files)} parquet files. Starting to stream...")

        MAX_ROWS_PER_FILE = 10000
        for file in parquet_files:
            filename = os.path.basename(file)
            print(f"Reading file: {filename}")
            try:
                df = pd.read_parquet(file).head(MAX_ROWS_PER_FILE)
                drop_cols = ['Label', 'source_file', 'target']
                for col in drop_cols:
                    if col in df.columns:
                        df = df.drop(columns=[col])
                
                columns = df.columns.tolist()
                for index, row in enumerate(df.itertuples(index=False, name=None)):
                    record = dict(zip(columns, row))
                    producer.send(topic_name, record)
                    
                    if index > 0 and index % 100 == 0:
                        print(f"Sent {index} records from {filename}...")
                    
                    # Simulate real-time delay (e.g., 50ms per record)
                    time.sleep(0.05)
                    
            except Exception as e:
                print(f"Error reading or sending data from {file}: {e}")

        producer.flush()
        print("Finished streaming all files.")
    finally:
        if producer is not None:
            print("Closing Kafka Producer connection...")
            producer.close()

if __name__ == "__main__":
    run_producer()
