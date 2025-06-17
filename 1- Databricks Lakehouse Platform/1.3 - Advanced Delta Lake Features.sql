-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

--USE CATALOG hive_metastore

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 6

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- Note: The following command has no effect in this case, as an optimization operation was automatically executed on the table in version 6.
OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- Note: The retentionDurationCheck configuration is not available on Serverless compute
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- Instead, use table properties
ALTER TABLE employees SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 0 hours')

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

--%fs ls '/path/to/employees'

-- COMMAND ----------

-- Note: You may still see results due to a cached version of the table in the serverless compute environment
SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

--%fs ls '/path/to/employees'
