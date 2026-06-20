import os
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

print("Initializing Spark session for smoke test...")
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Smoke_Test_Model") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

model_path = "./training_model/spark_rf_model"
print(f"Attempting to load model from: {model_path}")
try:
    model = PipelineModel.load(model_path)
    print("SUCCESS: PipelineModel loaded successfully!")
except Exception as e:
    print(f"FAILED: Could not load model: {e}")
finally:
    spark.stop()
