# Databricks notebook source
import dlt
import pyspark.sql.functions as F

datasets_path = spark.conf.get("datasets_path")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Bronze Layer Tables

# COMMAND ----------



@dlt.table(
    name = "books_bronze", 
    comment = "The raw books data, ingested from CDC feed"
)
def ingest_books_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{datasets_path}/books-cdc")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Silver Layer Tables

# COMMAND ----------

dlt.create_target_table(
    name = "books_silver")

dlt.apply_changes(
    target = "books_silver",
    source = "books_bronze",
    keys = ["book_id"],
    sequence_by = F.col("row_time"),
    apply_as_deletes = F.expr("row_status = 'DELETE'"),
    except_column_list = ["row_status", "row_time"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Gold Layer Tables

# COMMAND ----------

@dlt.table(
    comment="Number of books per author")
def author_counts_state():
    return (
        dlt.read("books_silver")
            .groupBy("author")
            .agg(F.count("*").alias("books_count"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Views

# COMMAND ----------

@dlt.view
def books_sales():
    return (
        dlt.read("orders_cleaned").withColumn("book", F.explode("books")).alias("o")
            .join(
                dlt.read("books_silver").alias("b"), 
                F.col("o.book.book_id") == F.col("b.book_id")
            )
    )
