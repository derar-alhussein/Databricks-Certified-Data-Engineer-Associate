-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Lab: Databases and Tables on Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To create external tables in Databricks Free Edition, you first need to set up a connection to an Amazon S3 bucket to store the table data.
-- MAGIC
-- MAGIC - Step 1: Create an S3 bucket in your AWS account
-- MAGIC - Step 2: Create an [IAM role and IAM policy](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials#step-1-create-an-iam-role) in the same account as your S3 bucket 
-- MAGIC - Step 3: Create a [Storage Credential](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials#step-2-give-databricks-the-iam-role-details) named "lakehouse_credential" in this Databricks workspace
-- MAGIC - Step 4: Update the [IAM role trusted policy](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials#step-3-update-the-iam-role-trust-relationship-policy) of your IAM role
-- MAGIC - Step 5: In the cells below, replace <BUCKET> with the name of your S3 bucket, and then proceed to run them.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Setting the default catalog
-- MAGIC
-- MAGIC Run the cell below to create and set the current catalog

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS lakehouse_custom_location URL 's3://<BUCKET>'
     WITH (CREDENTIAL lakehouse_credential)
     COMMENT 'my custom storage';

CREATE CATALOG IF NOT EXISTS demo_cat
MANAGED LOCATION 's3://<BUCKET>';

USE CATALOG demo_cat;

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

--------------------

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
-- MAGIC **s3://&lt;BUCKET&gt;/external_storage/actors_external**
-- MAGIC
-- MAGIC The schema for the table:
-- MAGIC
-- MAGIC | Column Name | Column Type |
-- MAGIC | --- | --- |
-- MAGIC | actor_id | INT |
-- MAGIC | name | STRING |
-- MAGIC | nationality | STRING |

-- COMMAND ----------

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3- Checking table metadata
-- MAGIC
-- MAGIC Review the extended metadata information of the table, and verify that:
-- MAGIC 1. The table type is External
-- MAGIC 1. The table is located under the directory: **s3://&lt;BUCKET&gt;/external_storage/actors_external**

-- COMMAND ----------

DESCRIBE EXTENDED actors_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q4- Dropping manged table
-- MAGIC
-- MAGIC Drop the manged table **movies_managed** 

-- COMMAND ----------

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check that the directory of the managed table has been deleted
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/movies_managed'

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

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Use the new schema to create the below **movies** table

-- COMMAND ----------

--------------------


CREATE TABLE movies
  (title STRING, category STRING, length INT, release_date DATE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q6- Creating new schema in custom location
-- MAGIC
-- MAGIC Create a new schema named **cinema_custom** in the directory: **s3://&lt;BUCKET&gt;/custom_schemas**

-- COMMAND ----------

--------------------

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
