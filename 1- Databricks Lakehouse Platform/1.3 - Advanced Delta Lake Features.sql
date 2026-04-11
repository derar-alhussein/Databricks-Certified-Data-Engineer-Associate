-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

USE CATALOG alfred_databricks

-- COMMAND ----------

DESCRIBE HISTORY bronze.employees

-- COMMAND ----------

SELECT * 
FROM bronze.employees VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM bronze.employees@v4

-- COMMAND ----------

DELETE FROM bronze.employees

-- COMMAND ----------

SELECT * FROM bronze.employees

-- COMMAND ----------

RESTORE TABLE bronze.employees TO VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM bronze.employees

-- COMMAND ----------

DESCRIBE HISTORY bronze.employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

DESCRIBE DETAIL bronze.employees

-- COMMAND ----------

OPTIMIZE bronze.employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL bronze.employees

-- COMMAND ----------

DESCRIBE HISTORY bronze.employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

VACUUM bronze.employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM bronze.employees RETAIN 0 HOURS

-- COMMAND ----------

-- no databricks free edition não temos acesso para alterar essa configuracao
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM bronze.employees RETAIN 0 HOURS

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT * FROM bronze.employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE bronze.employees

-- COMMAND ----------

SELECT * FROM bronze.employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'
