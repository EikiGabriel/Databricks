# Databricks notebook source
#df.write.format("delta") \
#.mode("overwrite") \
#.option("overwriteSchema", True) \
#.saveAsTable(f"{database_name}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tcc_silver.customer_transform
# MAGIC AS
# MAGIC SELECT * 
# MAGIC FROM tcc_bronze.customer
# MAGIC WHERE ModifiedDate <> "2005-07-01T00:00.000+0000"
