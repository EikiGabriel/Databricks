# Databricks notebook source
table = 'trips_data_user2'
database_name = 'tcc_bronze'
folder_path = "/mnt/adlslakehousedatabricks/landed/tcc_landed"

df_uber2 = spark.read.csv(
    f"{folder_path}/{table}.csv",
    header=True,
    inferSchema=True
)
display(df_uber)

# COMMAND ----------

df_uber2.write.format("delta").saveAsTable(f"{database_name}.data_uber_user2")
