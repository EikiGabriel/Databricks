# Databricks notebook source
table = 'trips_data_user3'
database_name = 'tcc_bronze'
folder_path = "/mnt/adlslakehousedatabricks/landed/tcc_landed"

df_uber3 = spark.read.csv(
    f"{folder_path}/{table}.csv",
    header=True,
    inferSchema=True
)
display(df_uber)

# COMMAND ----------

df_uber3.write.format("delta").saveAsTable(f"{database_name}.data_uber_user3")
