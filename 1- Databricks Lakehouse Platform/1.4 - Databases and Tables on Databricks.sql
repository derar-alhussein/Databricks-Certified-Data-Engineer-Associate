-- Databricks notebook source
-- MAGIC %md
-- MAGIC To create external tables in Unity Catalog, let's use the default external location:
-- MAGIC - Navigate to the Catalog explorer in the left sidebar.
-- MAGIC - At the top, click the **External Data** button.
-- MAGIC - Copy the URL and replace the `<EXTERNAL-URL>` placeholder below.
-- MAGIC
-- MAGIC The following cell configures the path as a notebook's parameter using Widgets, enabling its use in SQL queries through the `${external_location}` variable.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("external_location", '<EXTERNAL-URL>/external_storage')
-- MAGIC external_location = dbutils.widgets.get("external_location")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION '${external_location}/external_default';

INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # The underlying table directory is protected in Unity Catalog; therefore, this command cannot be executed.
-- MAGIC # display(dbutils.fs.ls('/path/to/managed_default'))

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f'{external_location}/external_default'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

USE SCHEMA new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION '${external_location}/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f'{external_location}/external_new_default'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location

-- COMMAND ----------

CREATE SCHEMA custom
MANAGED LOCATION '${external_location}/custom'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE SCHEMA custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION '${external_location}/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f'{external_location}/external_custom'))
