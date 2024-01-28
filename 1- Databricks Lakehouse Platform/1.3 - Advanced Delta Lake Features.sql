-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Advanced Delta Lake Features
-- MAGIC In this notebook we'll execute a few Delta Lake management commands like Time travel, `OPTIMIZE`, `VACUUM` and `DROP`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Time Travel
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting your table
-- MAGIC Now imagine that we delete our table.

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rolling back to previous table states
-- MAGIC Our latest table state has no records!
-- MAGIC How can we restore it?

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC And we have recovered the previous table version before the delete.

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE Command
-- MAGIC `OPTIMIZE` can helps organize the Delta table files in order to increase its performance.
-- MAGIC Usually this command merges small files into larger ones in order to prevent problems with IO with larges quantities of small files. <br>
-- MAGIC `ZORDER` is an index method that organizes your delta parquet files based on specific columns, helping you access data from your table.

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VACUUM Command
-- MAGIC And finally, the `VACCUM` command enables you to remove unused parquet files from your delta table, saving storage over your Data Lakehouse.<br>
-- MAGIC Just remember that `VACCUM` will hinder your ability to perform time travel ober the `RETAIN` period, so chose wisely.
-- MAGIC

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As the default retention period is 7 days, the `VACUUM` command did not changed the table (deleted parquet files).

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After changing the parameter `spark.databricks.delta.retentionDurationCheck.enabled = false` (just do it for the purpose of the demonstration, don't do it in production) we managed to execute `VACUUM` and remove the previous versions of the table. You can check this by trying to access the first table version.

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping Tables
-- MAGIC And to delete the Delta table you can use the `DROP` command.

-- COMMAND ----------

DROP TABLE employees


-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------


