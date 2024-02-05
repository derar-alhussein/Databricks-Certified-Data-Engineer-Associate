# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

print(f"{dataset_bookstore}")

# COMMAND ----------

dbutils.fs.ls(f"{dataset_bookstore}")

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
 ### OP Mode complete throws an error without streaming aggregations
 .outputMode("append")
 .table("orders_bronze"))

# COMMAND ----------

raw_dir = f"{dataset_bookstore}/orders-raw"
get_index(raw_dir)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view customers_lookup
# MAGIC as
# MAGIC select * from json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

(
    spark.readStream
    .table("orders_bronze")
    .createOrReplaceTempView("orders_bronze_tmp")
)

# COMMAND ----------

# MAGIC %run ../Includes//Orders_Bronze_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_enriched_tmp

# COMMAND ----------

(spark.table("orders_enriched_tmp")
 .writeStream
 .format("delta")
 .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
 .outputMode("append")
 .table("orders_silver")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

(
    spark.readStream
    .table("orders_silver")
    .createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %run ../Includes//daily_customer_books_tmp

# COMMAND ----------

(
    spark.table("daily_customer_books_temp")
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
    .trigger(availableNow=True)
    .table("daily_customer_books")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
