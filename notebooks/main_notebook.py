from src.ingestion.ingest import ingest_data          # Existing ingestion
from src.transformation.transformation import run_pipeline
from src.utils.spark_session import get_spark_session
from src.config_reader import read_config
import os
import yaml



# 1️⃣ Spark session + config
spark = get_spark_session()
config = read_config()

# 2️⃣ Ingest raw → bronze
print("🚀 Starting ingestion to Bronze")
ingest_data()
print("✅ Bronze ingestion completed!")

# 3️⃣ Bronze → Silver → Gold transformations
print("🚀 Starting Bronze → Silver → Gold transformations")
run_pipeline()   # Reads from Bronze, writes to Silver & Gold
print("🎉 ETL Pipeline completed successfully!")
