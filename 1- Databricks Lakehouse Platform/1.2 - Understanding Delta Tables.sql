-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

DROP TABLE employees_new;

-- COMMAND ----------

CREATE TABLE employees_new
  (id INT, name STRING, age INT, salary DOUBLE); 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees_new** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- NOTE: With latest Databricks Runtimes, inserting few records in single transaction is optimized into single data file.
-- For this demo, we will insert the records in multiple transactions in order to create 4 data files.

INSERT INTO employees_new
VALUES 
  (1, "Adam", 42, 3500.0),
  (2, "Sarah", 32, 4020.5);

INSERT INTO employees_new
VALUES
  (3, "John", 19, 2999.3),
  (4, "Thomas", 23, 4000.3);

INSERT INTO employees_new
VALUES
  (5, "Anna", 56, 2500.0);

INSERT INTO employees_new
VALUES
  (6, "Kim", 34, 6200.3)

-- NOTE: When executing multiple SQL statements in the same cell, only the last statement's result will be displayed in the cell output.

-- COMMAND ----------

SELECT * FROM employees_new

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL employees_new

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_new'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees_new 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees_new   

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees_new

-- COMMAND ----------

SELECT * FROM employees_new

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY employees_new

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees_new/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees_new/_delta_log/00000000000000000005.json'

-- COMMAND ----------


