from src.utils.spark_session import get_spark_session
from src.config_reader import read_config,get_input_files
from delta.tables import DeltaTable
import os

def ingest_data():
  spark = get_spark_session()
  config = read_config()
  raw_path = config["paths"]["raw"]
  bronze_path = config["paths"]["bronze"]
  input_files = get_input_files()

  for file_name in input_files:
    file_path = os.path.join(raw_path,file_name)
    table_name = os.path.splitext(file_name)[0]
    table_name_clean = re.sub(r'\d+$', '', table_name)
    df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(file_path)
    #standardize columns
    df = standardize_columns(df)
    #write to Bronze layer with table-specific folder
    full_bronze_path = os.path.join(bronze_path,table_name_clean)
    if DeltaTable.isDeltaTable(spark,full_bronze_path):
      bronze_table = DeltaTable.forPath(spark,full_bronze_path)
      bronze_table.alias("t").merge(df.alias("s"),"t.ingestion_date = s.ingestion_date").whenNotMatchedInsertAll().execute()
    else:
      df.write.format("delta").mode("overwrite").save(full_bronze_path)
      
    print(f"Ingested File:{file_name} -> Bronze:{full_bronze_path}")
    
  print("All Files Ingested Successfully")

if __name__ == "__main__":
  ingest_data()
      
