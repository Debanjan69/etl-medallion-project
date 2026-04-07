from pyspark.sql.functions import current_date

def standardize_columns(df):
    # 1️⃣ Standardize column names
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    
    # 2️⃣ Remove duplicate rows
    df = df.dropDuplicates()
    
    # 3️⃣ Add ingestion date column
    df = df.withColumn("ingestion_date", current_date())
    
    return df
