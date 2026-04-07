from src.utils.spark_session import get_spark_session
from src.utils.helpers import standardize_columns
from src.config_reader import read_config

def ingest_data():
    spark = get_spark_session()
    config = read_config()

    df = spark.read.csv(
        config["paths"]["raw"] + config["files"]["input_file"],
        header=True,
        inferSchema=True
    )

    df = standardize_columns(df)
  
    df.write.format("delta") \
        .mode("overwrite") \
        .save(config["paths"]["bronze"])

    

if __name__ == "__main__":
    ingest_data()
      
