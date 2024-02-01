-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/orders-json-raw")

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT  "This is my build from scratch"
AS SELECT * FROM cloud_files("dbfs:/mnt/demo-datasets/bookstore/orders-json-raw","json",map("cloudFiles.inferColumnTypes","true"))
