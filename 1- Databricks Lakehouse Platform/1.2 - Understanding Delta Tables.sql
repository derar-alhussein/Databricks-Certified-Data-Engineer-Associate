-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

USE CATALOG hive_metastore

-- COMMAND ----------

CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- NOTE: With latest Databricks Runtimes, inserting few records in single transaction is optimized into single data file.
-- For this demo, we will insert the records in multiple transactions in order to create 4 data files.

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- NOTE: When executing multiple SQL statements in the same cell, only the last statement's result will be displayed in the cell output.

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata and Directory

-- COMMAND ----------
-- List metadata and contents of employees table, database directory and employees table in dbfs

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------
-- Increase the salary of all employees starting with A

-- COMMAND ----------
-- List metadata and contents of employees table, database directory and employees table in dbfs

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------
-- Show the version history of employees, and delta log, and data files in dbfs