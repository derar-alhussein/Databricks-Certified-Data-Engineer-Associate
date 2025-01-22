-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

-- list history of employees

-- COMMAND ----------

-- show contents of employees as of version 4 

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- restore employees to version 5

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

-- 
DESCRIBE DETAIL employees

-- COMMAND ----------

-- z-order employees by id and show the outcome using Describe and file contents in dbfs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

-- vaccum employees and show the outcome using Describe and file contents in dbfs
-- vacuum employees with retention of 0 hours and show the outcome using Describe and file contents in dbfs

-- COMMAND ----------

-- drop employees and show the outcome using Describe and file contents in dbfs
