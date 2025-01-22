-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

USE CATALOG hive_metastore;

-- create a mananged table called managed_default (width, length, height)

-- create an external table called managed_default (width, length, height)

-- COMMAND ----------

-- COMMAND ----------

-- demonstrate differences between managed_default and external when dropped

-- COMMAND ----------

-- create a new schema and show locations of managed/external tables

-- create a new schema with a custom location and show locations of managed/external tables
