def standardize_columns(df):
  for col in df.columns:
    df = df.withColumnRenamed(col,col.lower())
  return df
    
