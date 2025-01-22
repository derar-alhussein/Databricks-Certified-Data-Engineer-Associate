-- Databricks notebook source
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- create a streaming table `books_bronze` for the CDC feed in books-cdc/, using autoloader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

-- create a streaming table books_silver and apply the CDC changes; also apply delete operations

-- COMMAND ----------

-- create a delta table (non-streaming) of author_counts from books_silver

-- COMMAND ----------

-- create a DLT view of books sales based on orders_cleaned and books_silver views
