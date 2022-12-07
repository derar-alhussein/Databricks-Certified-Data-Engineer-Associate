# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Delta Live Tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

datasets_path = spark.conf.get("datasets_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### orders_raw

# COMMAND ----------

@dlt.table
def orders_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{datasets_path}/orders-json-raw")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### customers

# COMMAND ----------

@dlt.table(
  comment = "The customers lookup table, ingested from customers-json")
def customers():
    return (
        spark.read
            .format("json")
            .load(f"{datasets_path}/customers-json")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Silver Layer Tables
# MAGIC 
# MAGIC #### orders_cleaned

# COMMAND ----------

@dlt.table(
    comment = "The cleaned books orders with valid order_id")
@dlt.expect_or_drop("valid_order_number", F.col("order_id").isNotNull())
def orders_cleaned():
    return (
        dlt.read_stream("orders_raw").alias("o")
            .join(dlt.read("customers").alias("c"), ["customer_id"], "left")
            .withColumn("order_timestamp", F.from_unixtime(F.col("order_timestamp"), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
            .select(
                "order_id",
                "quantity",
                "o.customer_id",
                "c.email",
                "order_timestamp",
                "o.books"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC >> Constraint violation
# MAGIC 
# MAGIC | Expectation | Behavior |
# MAGIC | --- | --- |
# MAGIC | **`@dlt.expect_or_drop`** | Discard records that violate constraints |
# MAGIC | **`@dlt.expect_or_fail`** | Violated constraint causes the pipeline to fail  |
# MAGIC | **`@dlt.expect`** | Records violating constraints will be kept, and reported in metrics |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Gold Tables

# COMMAND ----------

@dlt.table(
    comment = "Daily number of books per customer in China")
def cn_daily_customer_books():
    return (
        dlt.read("orders_cleaned")
            .withColumn("order_date", F.to_date("order_timestamp"))
            .groupBy("customer_id", "email", "order_date")
            .agg(F.sum("quantity").alias("books_counts"))
    )
