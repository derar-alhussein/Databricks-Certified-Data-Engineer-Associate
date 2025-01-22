# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# using spark api load orders_raw into a temporary view orders_raw_temp

# COMMAND ----------

# enrich orders_raw_tmp as orders_tmp, by adding ingestion timestamp and source file

# COMMAND ----------

# using spark api, create a bronze table 'orders_bronze' from the view orders_tmp

# COMMAND ----------

load_new_data()


# COMMAND ----------

# using spark api, create a static customer lookup table from customers-json

# using spark create a streaming view orders_bronze_tmp from orders_bronze

# using SQL create orders_enriched_tmp view based on orders_bronze_tmp and customers_lookup:
#    - replace profile with first_name and last_name
#    - change unix timestamp to yyyy-mm-dd hh:mm:ss format
#    - exclude entries with no quantity

# COMMAND ----------

# using spark, persist orders_enriched_tmp to orders_silver

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

# using spark api, create orders_silver_tmp view from orders_silver

# COMMAND ----------

# using SQL, create a view for the number of books sold by day of the month, by customer

# COMMAND ----------

# using spark api, persist this to a gold table `daily_customer_books`

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
