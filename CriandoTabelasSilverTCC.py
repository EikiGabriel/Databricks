# Databricks notebook source
#df.write.format("delta") \
#.mode("overwrite") \
#.option("overwriteSchema", True) \
#.saveAsTable(f"{database_name}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tcc_silver.products_transform
# MAGIC SELECT * 
# MAGIC FROM tcc_bronze.products
# MAGIC WHERE ModifiedDate <> '2005-07-01 00:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tcc_bronze.products_test
# MAGIC RENAME TO tcc_bronze.products
