-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

create table orders as 
select * from parquet. `${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

Create or replace table orders as
select * from parquet. `${dataset.bookstore}/orders`


-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select * from parquet. `${dataset.bookstore}/orders`

-- COMMAND ----------

create table customers
select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

Create or replace TEMP VIEW vw_customer as
select * from json.`${dataset.bookstore}/customers-json-new`

-- COMMAND ----------

select * from vw_customer

-- COMMAND ----------

Merge into customers a
using vw_customer b
on a.customer_id = b.customer_id
when matched And a.email is null and b.email is not null then
  update set email = b.email, updated = b.updated
when not matched then insert *


-- COMMAND ----------

create or replace temp view books_updates
(book_id string, title string, author string, category string, price double)
using csv
options (header = 'true', delimiter = ';', path = '${dataset.bookstore}/books-csv-new')


-- COMMAND ----------

select * from books_updates

-- COMMAND ----------

merge into books a
using books_updates b
on a.book_id = b.book_id
when not matched and b.category = 'Computer Science' then insert *

-- COMMAND ----------

select * from books

-- COMMAND ----------


