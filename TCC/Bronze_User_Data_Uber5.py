# Databricks notebook source
table = 'trips_data_user5'
database_name = 'tcc_bronze'
folder_path = "/mnt/adlslakehousedatabricks/landed/tcc_landed"

df_uber = spark.read.parquet(f"{folder_path}/{table}.parquet")
display(df_uber)

# COMMAND ----------

table = 'data_uber_user5'
database_name = 'tcc_bronze'


df_uber.write.format("delta").saveAsTable(f"{database_name}.data_uber_user5")
