def ingest_data(spark,path):
  df = spark.read.csv(path,header = True,
      inferSchema = True)
  return df
