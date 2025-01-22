-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- create orders table from parquet files in /orders'

-- overwrite the table and show the results in history using a) CRTAS, b) INSERT OVERWRITE

-- append new data to the table

-- COMMAND ----------

-- update customers-json with customers-json-new by setting the email address if it was previously null, and setting the 'updated' column accordingly

-- COMMAND ----------

-- insert all new "Computer Science" books from books-csv-new into books
