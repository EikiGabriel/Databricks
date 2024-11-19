# Databricks notebook source
#df.write.format("delta") \
#.mode("overwrite") \
#.option("overwriteSchema", True) \
#.saveAsTable(f"{database_name}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM tcc_silver.customer_transform
