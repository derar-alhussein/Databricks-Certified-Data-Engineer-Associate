-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab: Querying Files

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

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the above SELECT query in a CTAS statement to create the table **enrollments**

-- COMMAND ----------

--------------------

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

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
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

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the manged table **courses** from the temporary view **courses_tmp_vw**

-- COMMAND ----------

--------------------

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the below cell to ensure data was written as expected in the **courses** table

-- COMMAND ----------

SELECT * FROM courses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Finally, review the metadata information of the table **courses**, and verify that the table type is Managed

-- COMMAND ----------

DESCRIBE EXTENDED courses
