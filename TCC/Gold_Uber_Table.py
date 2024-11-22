# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tcc_gold.UberViagens
# MAGIC AS 
# MAGIC SELECT * FROM tcc_silver.data_uber_user1
# MAGIC UNION ALL
# MAGIC SELECT * FROM tcc_silver.data_uber_user2
# MAGIC UNION ALL
# MAGIC SELECT * FROM tcc_silver.data_uber_user3
# MAGIC UNION ALL
# MAGIC SELECT * FROM tcc_silver.data_uber_user4
# MAGIC UNION ALL
# MAGIC SELECT * FROM tcc_silver.data_uber_user5;
