# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo/checkpoints/")

# COMMAND ----------

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format","parquet")
  .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/checkpoints/orders_raw")
  .load(f"{dataset_bookstore}/orders-raw")
  .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_temp AS(
# MAGIC   select *, current_timestamp() arrival_time, input_file_name() source_file from orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_temp

# COMMAND ----------

(spark.table("orders_temp")
 .writeStream
 .format("delta")
 .option("checkpointLocation","dbfs:/mnt/demo/checkpoints/orders-bronze")
 .outputMode("append")
 .table("orders_bronze"))
