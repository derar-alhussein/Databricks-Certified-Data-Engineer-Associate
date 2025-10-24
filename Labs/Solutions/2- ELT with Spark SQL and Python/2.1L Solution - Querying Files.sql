-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab Solution: Querying Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to setup the lab environment

-- COMMAND ----------

-- MAGIC %run ../Includes/Setup-Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1- Extracting data directly from Parquet files
-- MAGIC
-- MAGIC Use a SELECT statement to directly query the content of the Parquet files in the directory **${dataset.school}/enrollments**

-- COMMAND ----------

-- ANSWER
SELECT * FROM parquet.`${dataset.school}/enrollments`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the above SELECT query in a CTAS statement to create the table **enrollments**

-- COMMAND ----------

-- ANSWER
CREATE TABLE enrollments AS
SELECT * FROM parquet.`${dataset.school}/enrollments`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Run the below cell to ensure data was written as expected in the **enrollments** table

-- COMMAND ----------

SELECT * FROM enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2- Registering Tables from JSON Files
-- MAGIC
-- MAGIC Use CTAS statement to create the table **students** from the json files in the directory: **${dataset.school}/students-json**
-- MAGIC

-- COMMAND ----------

-- ANSWER
CREATE TABLE students AS
SELECT * FROM json.`${dataset.school}/students-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the below cell to ensure data was written as expected in the **students** table

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3- Registering Tables from CSV Files
-- MAGIC
-- MAGIC Create the temporary view **courses_tmp_vw** from the csv files in the directory: **${dataset.school}/courses-csv**
-- MAGIC
-- MAGIC Knowing that:
-- MAGIC * The delimiter is semicolon (**;**)
-- MAGIC * There is a header of column names in each file
-- MAGIC
-- MAGIC The schema for the view:
-- MAGIC
-- MAGIC | Column Name | Column Type |
-- MAGIC | --- | --- |
-- MAGIC | course_id | STRING |
-- MAGIC | title | STRING |
-- MAGIC | instructor | STRING |
-- MAGIC | category | STRING |
-- MAGIC | price | DOUBLE |

-- COMMAND ----------

-- ANSWER
CREATE TEMP VIEW courses_tmp_vw
   (course_id STRING, title STRING, instructor STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.school}/courses-csv",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the manged table **courses** from the temporary view **courses_tmp_vw**

-- COMMAND ----------

-- ANSWER
CREATE TABLE courses AS
  SELECT * FROM courses_tmp_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Query the data in the **courses** table to ensure data was written as expected.

-- COMMAND ----------

-- ANSWER
SELECT * FROM courses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Finally, review the metadata information of the table **courses**, and verify that the table type is Managed

-- COMMAND ----------

DESCRIBE EXTENDED courses
