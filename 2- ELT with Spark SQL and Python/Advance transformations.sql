-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

select customer_id, profile:first_name
from customers

-- COMMAND ----------


