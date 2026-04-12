-- Databricks notebook source
USE CATALOG alfred_databricks;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

/*
[UC_HIVE_METASTORE_DISABLED_EXCEPTION] The operation attempted to use Hive Metastore, which is disabled due to legacy features being turned off in your account or workspace. Please enable Unity Catalog as the Hive Metastore is disabled.
SELECT * FROM global_temp.global_temp_view_latest_phones;
*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
-- DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


