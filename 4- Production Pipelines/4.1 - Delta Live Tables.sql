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

-- in SQL, create a streaming table from orders-json-raw, using autoloader

-- in SQL, create a streaming table from customers-json, using CTAS

-- COMMAND ----------

-- create `orders_cleaned` (silver table):
  -- add a constraint for not null order id, and drop vioaliting rows
  -- from profile extract first_name, last_name, country
  -- cast the timestamp to `yyyy-mm-dd HH:mm:ss` format
  -- enrich by joining with customers table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables

-- COMMAND ----------

-- create a gold streaming table cn_daily_custmers_books, showing daily book counts by customers in China
