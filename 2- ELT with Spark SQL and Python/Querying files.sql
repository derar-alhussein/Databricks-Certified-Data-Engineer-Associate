-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select *,
  input_file_name() as file_name
  from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select * from csv. `dbfs:/mnt/demo-datasets/bookstore/books-csv`

-- COMMAND ----------

Create table books_csv
(book_id string, title string, author string, category string, price double)
using csv
options (header='true', delimiter = ';')
location 'dbfs:/mnt/demo-datasets/bookstore/books-csv'

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

create table books 
as select * from books_csv

-- COMMAND ----------

describe extended books

-- COMMAND ----------

describe history books

-- COMMAND ----------


