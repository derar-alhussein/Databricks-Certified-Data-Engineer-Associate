-- Databricks notebook source
-- MAGIC %md
-- MAGIC To create external tables in Databricks Express or Free Edition, you first need to set up a connection to an Amazon S3 bucket to store the table data.
-- MAGIC
-- MAGIC - Step 1: Create an S3 bucket in your AWS account
-- MAGIC - Step 2: Configure [External Location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/external-locations#-option-1-create-an-external-location-for-an-s3-bucket-using-an-aws-cloudformation-template) object in this workspace to connect your S3 bucket to Databricks
-- MAGIC - Step 3: In the cells below, replace _&lt;BUCKET&gt;_ with the name of your S3 bucket, and then proceed to run them.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo_cat
MANAGED LOCATION 's3://<BUCKET>';

USE CATALOG demo_cat;

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
LOCATION 's3://<BUCKET>/external_storage/external_default';
  
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

SELECT * FROM managed_default

-- COMMAND ----------

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE SCHEMA new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 's3://<BUCKET>/external_storage/external_new_default';
  
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

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location

-- COMMAND ----------

CREATE SCHEMA custom
MANAGED LOCATION 's3://<BUCKET>/custom_schemas'

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
LOCATION 's3://<BUCKET>/external_storage/external_custom';
  
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

-- Note: It is not permitted to list the files of managed tables. You may examine the table files directly in your S3 bucket.
--%fs ls '/path/to/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 's3://<BUCKET>/external_storage/external_custom'
