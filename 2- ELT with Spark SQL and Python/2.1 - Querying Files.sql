-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- query the contents of customers-json (2 ways)

-- COMMAND ----------

-- query the contents of files in customers-json

-- add a colum with the source file name

-- query the contents as text, binary 

-- query the contents of books-csv/ as csv

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

-- create an external table from books-csv

-- is this a delta table?

-- is it updated when we add new files?

-- use the pyspark API to write a new csv file in books-csv/

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- create a delta table from customers-json

-- create a delta table from books-csv
