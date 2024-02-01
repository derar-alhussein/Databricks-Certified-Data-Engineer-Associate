# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/books-cdc/")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from json.`${dataset.bookstore}/books-cdc/02.json`

# COMMAND ----------


