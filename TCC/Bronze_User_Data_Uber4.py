# Databricks notebook source
table = 'trips_data_user4'
database_name = 'tcc_bronze'
folder_path = "/mnt/adlslakehousedatabricks/landed/tcc_landed"

df_uber4 = spark.read.csv(
    f"{folder_path}/{table}.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

table = 'data_uber_user4'
database_name = 'tcc_bronze'


df_uber4.write.format("delta").saveAsTable(f"{database_name}.data_uber_user4")
