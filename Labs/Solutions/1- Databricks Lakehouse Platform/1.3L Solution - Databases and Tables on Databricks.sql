-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lab Solution: Databases and Tables on Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1 - Creating managed table
-- MAGIC
-- MAGIC In the default database, create a managed table named **movies_managed** that has the following schema:
-- MAGIC
-- MAGIC
-- MAGIC | Column Name | Column Type |
-- MAGIC | --- | --- |
-- MAGIC | title | STRING |
-- MAGIC | category | STRING |
-- MAGIC | length | FLOAT |
-- MAGIC | release_date | DATE |

-- COMMAND ----------

-- Answer
CREATE TABLE movies_managed
  (title STRING, category STRING, length INT, release_date DATE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Review the extended metadata information of the table, and verify that:
-- MAGIC 1. The table type is Managed
-- MAGIC 1. The table is located under the default hive directory

-- COMMAND ----------

DESCRIBE EXTENDED movies_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2 - Creating external table
-- MAGIC
-- MAGIC In the default database, create an external Delta table named **actors_external**, and located under the directory:
-- MAGIC **dbfs:/mnt/demo/actors_external**
-- MAGIC
-- MAGIC The schema for the table:
-- MAGIC
-- MAGIC | Column Name | Column Type |
-- MAGIC | --- | --- |
-- MAGIC | actor_id | INT |
-- MAGIC | name | STRING |
-- MAGIC | nationality | STRING |

-- COMMAND ----------

-- Answer
CREATE OR REPLACE TABLE actors_external
  (actor_id INT, name STRING, nationality STRING)
LOCATION 'dbfs:/mnt/demo/actors_external';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q4- Checking table metadata
-- MAGIC
-- MAGIC Review the extended metadata information of the table, and verify that:
-- MAGIC 1. The table type is External
-- MAGIC 1. The table is located under the directory: **dbfs:/mnt/demo/actors_external**

-- COMMAND ----------

DESCRIBE EXTENDED actors_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3- Dropping manged table
-- MAGIC
-- MAGIC Drop the manged table **movies_managed** 

-- COMMAND ----------

-- Answer
DROP TABLE movies_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that the directory of the managed table has been deleted
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/actors_external'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q4- Drop external table
-- MAGIC
-- MAGIC Drop the external table **actors_external** 

-- COMMAND ----------

DROP TABLE actors_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that the directory of the external table has **Not** been deleted

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/actors_external'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q5- Creating new schema
-- MAGIC
-- MAGIC Create a new schema named **db_cinema**

-- COMMAND ----------

-- Answer
CREATE SCHEMA db_cinema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Review the extended metadata information of the database, and verify that the database is located under the default hive directory.
-- MAGIC
-- MAGIC Note that the database folder has the extenstion **.db**

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED db_cinema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Use the new schema to create the below **movies** table

-- COMMAND ----------

-- Answer
USE db_cinema;

CREATE TABLE movies
  (title STRING, category STRING, length INT, release_date DATE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q6- Creating new schema in custom location
-- MAGIC
-- MAGIC Create a new schema named **cinema_custom** in the directory: **dbfs:/Shared/schemas/cinema_custom.db**

-- COMMAND ----------

-- Answer
CREATE SCHEMA cinema_custom
LOCATION 'dbfs:/Shared/schemas/cinema_custom.db'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the new schema to create the below **movies** table

-- COMMAND ----------

USE cinema_custom;

CREATE TABLE movies
  (title STRING, category STRING, length INT, release_date DATE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, review the extended metadata information of the table **movies**, and verify that:
-- MAGIC
-- MAGIC 1. The table type is Managed
-- MAGIC 1. The table is located in the new database defined under the custom location

-- COMMAND ----------

DESCRIBE EXTENDED movies
