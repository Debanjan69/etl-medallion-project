from src.utils.spark_session import get_spark_session
from src.utils.helpers import standardize_columns
from src.config_reader import read_config
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import when


def transform_to_silver():
    spark = get_spark_session()
    config = read_config()

    df = spark.read.format("delta").load(config["paths"]["bronze"])
    df = to_upper(df)
    #type casting and validation
    df = df.withColumn("age", col("age").cast(IntegerType()))
    df = df.withColumn("salary", col("salary").cast(DoubleType()))
    #filter invalid rows
    df = df.filter((col("age") > 0) & (col("salary") >= 0))
    #age group segmentation
    df = df.withColumn("age_group", 
        when(col("age") < 25, "Young")
        .when((col("age") >= 25) & (col("age") < 40), "Adult")
        .otherwise("Senior")
    )
    #salary bucket
    df = df.withColumn("salary_bucket", 
        when(col("salary") < 50000, "Low")
        .when((col("salary") >= 50000) & (col("salary") < 100000), "Medium")
        .otherwise("High")
    )
    # Write to Silver (partitioned by country)
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("country") \
        .save(config["paths"]["silver"])

    print("✅ Silver layer transformation complete")

if __name__ == "__main__":
    transform_to_silver()
