# Databricks notebook source
table = 'customer'
database_name = 'tcc_bronze'


folder_path = "/mnt/adlslakehousedatabricks/landed/tcc_landed"

# COMMAND ----------

df = spark.read.parquet(f"{folder_path}/{table}.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable(f"{database_name}.{table}")
