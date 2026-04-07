from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.utils.spark_session import get_spark_session
from src.config_reader import read_config
import os

def transform_to_gold(top_n==5):
    spark = get_spark_session()
    config = read_config()

    silver_path = config["paths"]["silver"]
    gold_path = config["paths"]["gold"]

    # 1️⃣ Read Silver table
    df = spark.read.format("delta").load(silver_path)

    # 2️⃣ Total salary per country and customer segment
    total_salary_df = df.groupBy("country", "customer_segment") \
                        .agg(F.sum("salary").alias("total_salary"))

    # 3️⃣ Average age per customer segment
    avg_age_df = df.groupBy("customer_segment") \
                   .agg(F.avg("age").alias("avg_age"))

    # 4️⃣ Top N customers per country by salary
    window_spec = Window.partitionBy("country").orderBy(F.desc("salary"))
    top_customers_df = df.withColumn("rank", F.row_number().over(window_spec)) \
                          .filter(F.col("rank") <= top_n) \
                          .drop("rank")
  from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.utils.spark_session import get_spark_session
from src.config_reader import read_config
import os

# --------------------------
# Silver → Gold Transformations
# --------------------------
def gold_layer_transform(top_n=5):
    spark = get_spark_session()
    config = read_config()

    silver_path = config["paths"]["silver"]
    gold_path = config["paths"]["gold"]

    # 1️⃣ Read Silver table
    df = spark.read.format("delta").load(silver_path)

    # 2️⃣ Total salary per country and customer segment
    total_salary_df = df.groupBy("country", "customer_segment") \
                        .agg(F.sum("salary").alias("total_salary"))

    # 3️⃣ Average age per customer segment
    avg_age_df = df.groupBy("customer_segment") \
                   .agg(F.avg("age").alias("avg_age"))

    # 4️⃣ Top N customers per country by salary
    window_spec = Window.partitionBy("country").orderBy(F.desc("salary"))
    top_customers_df = df.withColumn("rank", F.row_number().over(window_spec)) \
                          .filter(F.col("rank") <= top_n) \
                          .drop("rank")

    # 5️⃣ Merge into Gold Delta Table (incremental write)
    gold_table_path = os.path.join(gold_path, "customers_gold")
    if DeltaTable.isDeltaTable(spark, gold_table_path):
        gold_table = DeltaTable.forPath(spark, gold_table_path)
        gold_table.alias("t").merge(
            df.alias("s"),
            "t.customer_id = s.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(gold_table_path)

    # 6️⃣ Save aggregated metrics for analytics
    total_salary_df.write.format("delta").mode("overwrite").save(os.path.join(gold_path, "total_salary"))
    avg_age_df.write.format("delta").mode("overwrite").save(os.path.join(gold_path, "avg_age"))
    top_customers_df.write.format("delta").mode("overwrite").save(os.path.join(gold_path, "top_customers"))

    print("✅ Gold layer transformations complete")
    return total_salary_df, avg_age_df, top_customers_df
