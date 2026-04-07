from src.utils.spark_session import get_spark_session
from src.utils.helpers import standardize_columns
from src.config_reader import read_config, get_input_file
from delta.tables import DeltaTable
import os

def ingest_data():
    spark = get_spark_session()
    config = read_config()
    raw_path = config["paths"]["raw"]
    bronze_path = config["paths"]["bronze"]
    file_name = get_input_file()
    full_bronze_path = os.path.join(bronze_path, os.path.splitext(file_name)[0])

    # Read CSV
    df = spark.read.csv(os.path.join(raw_path, file_name), header=True, inferSchema=True)
    df = standardize_columns(df)

    # Upsert if Delta exists, else write new
    if DeltaTable.isDeltaTable(spark, full_bronze_path):
        bronze_table = DeltaTable.forPath(spark, full_bronze_path)
        bronze_table.alias("t") \
            .merge(df.alias("s"), "t.ingestion_date = s.ingestion_date") \
            .whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(full_bronze_path)

    print(f"✅ Ingested {file_name} to Bronze at {full_bronze_path}")
    

if __name__ == "__main__":
    ingest_data()
      
