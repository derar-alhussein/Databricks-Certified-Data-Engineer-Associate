# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from json.`${dataset_bookstore}/books-cdc/02.json`

# COMMAND ----------


