-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream.
-- MAGIC  format("cloudFiles")
-- MAGIC  .option("cloudFiles.format","parquet")
-- MAGIC  .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/orders_checkpoint")
-- MAGIC  .load(f"{dataset_bookstore}/orders-raw")
-- MAGIC .writeStream
-- MAGIC .option("checkpointLocation","dbfs:/mnt/demo/orders_checkpoint")
-- MAGIC .table("orders_raw")
-- MAGIC )
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
FROM orders_raw o
LEFT JOIN LIVE.customers c
ON o.customer_id= c.customer_id

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
from LIVE.orders_cleaned
WHERE country="China"
GROUP BY  customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------


