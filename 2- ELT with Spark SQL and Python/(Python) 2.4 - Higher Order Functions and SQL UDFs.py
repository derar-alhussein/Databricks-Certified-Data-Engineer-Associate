# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders
# MAGIC AS SELECT *
# MAGIC FROM parquet.`dbfs:/mnt/demo-datasets/bookstore/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filtering Arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, choosen_books
# MAGIC FROM 
# MAGIC (SELECT
# MAGIC order_id,
# MAGIC books,
# MAGIC FILTER (books, i -> i.subtotal >= 35) AS choosen_books
# MAGIC FROM orders)
# MAGIC WHERE size(choosen_books) > 0
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, multiple_copies
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC   FROM orders)
# MAGIC WHERE size(multiple_copies) > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Transforming Arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   TRANSFORM (
# MAGIC     books,
# MAGIC     b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC   ) AS subtotal_after_discount
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Functions (UDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers
# MAGIC AS SELECT *
# MAGIC FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_domain(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN (split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN concat("https://www.", split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_domain(email) domain
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN email like "%.com" THEN ("Commercial business")
# MAGIC   WHEN email like "%.org" THEN ("Non-profits organization")
# MAGIC   WHEN email like "%.edu" THEN ("Educational institution")
# MAGIC   ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC END;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import udf, when, col, split
from pyspark.sql.types import StringType

def site_type(email):
    if email is None:
        return None
    elif email.endswith('.com'):
        return "Commercial business"
    elif email.endswith('.edu'):
        return "Educational institution"
    elif email.endswith('.org'):
        return "Non-profits organization"
    else:
        domain = email.split("@")[1] if "@" in email else email
        return f"Unknown extension for domain: {domain}"

site_type_udf = udf(site_type, StringType())
spark.udf.register("site_type", site_type, StringType()) # to use with SQL as well

orders_df = spark.read.table("orders")
result_df = orders_df.withColumn("site_category", site_type_udf(col("email")))
display(result)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) as domain_category
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION get_domain;
# MAGIC DROP FUNCTION site_type;
