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

-- MAGIC %python
-- MAGIC # Configure the dataset path as a notebook parameter using Widgets, enabling its use in SQL queries through the ${dataset_bookstore} placeholder.
-- MAGIC dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

 SELECT *,
    -- input_file_name() source_file
    _metadata.file_path  source_file
  FROM json.`${dataset_bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`${dataset_bookstore}/books-csv`

-- COMMAND ----------

SELECT * FROM read_files(
    '${dataset_bookstore}/books-csv/export_*.csv',
    format => 'csv',
    header => 'true',
    delimiter => ';');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset_bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset_bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TABLE books AS
SELECT * FROM read_files(
    '${dataset_bookstore}/books-csv/export_*.csv',
    format => 'csv',
    header => 'true',
    delimiter => ';');
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
