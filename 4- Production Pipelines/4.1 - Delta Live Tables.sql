-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import dlt
-- MAGIC
-- MAGIC @dlt.table(
-- MAGIC     name="orders_raw"
-- MAGIC     comment=" The raw books orders, ingested from orders-raw"
-- MAGIC )
-- MAGIC def orders_raw():
-- MAGIC     return (
-- MAGIC         spark.readStream.format("cloudFiles")
-- MAGIC         .option("cloudFiles.format","json")
-- MAGIC         .option("cloudFiles.inferColumntTypes", "true")
-- MAGIC         .load(f"{datasets.path}/orders-json-raw")
-- MAGIC     )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files(
  "${datasets.path}/orders-json-raw",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.table(
-- MAGIC     name="customer"
-- MAGIC     comment="The customers lookup table, ingested from customer-json"
-- MAGIC )
-- MAGIC def customer():
-- MAGIC     return(
-- MAGIC         spark.read.format("json").load(f"{datasets.path}/customer-json"
-- MAGIC     )

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC #### orders_cleaned

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import dlt
-- MAGIC @dlt.table(
-- MAGIC     name="orders_cleaned"
-- MAGIC     comment="The cleaned books orders with valid order_id"
-- MAGIC )
-- MAGIC @dlt.expect_or_fail("valid_order_number","order_id IS NOT NULL")
-- MAGIC def orders_cleaned():
-- MAGIC     orders_raw = dlt.read("orders_raw")
-- MAGIC     customers = dlt.read("customers")
-- MAGIC
-- MAGIC     return (
-- MAGIC         orders_raw.alias("o")
-- MAGIC         .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
-- MAGIC         .select(
-- MAGIC             col("o.order_id"),
-- MAGIC             col("o.quantity"),
-- MAGIC             col("o.customer_id"),
-- MAGIC             col("c.profile.first_name").alias("f_name"),
-- MAGIC             col("c.profile.last_name").alias("l_name"),
-- MAGIC             from_unixtime(col("o.order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").alias("order_timestamp"),
-- MAGIC             col("o.books"),
-- MAGIC             col("c.profile.address.country").alias("country")
-- MAGIC         )
-- MAGIC     )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
    FROM STREAM(LIVE.orders_raw) o
    LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------


