-- Databricks notebook source
Create table managed_table
(id integer, name string)

-- COMMAND ----------

insert into managed_table values('1','Add'), ('2','Update')

-- COMMAND ----------

describe extended managed_table

-- COMMAND ----------

create table external_tab
(id int, name string)
location 'dbfs:/mnt/demo/external_table'

-- COMMAND ----------

insert into external_tab
values (1,'df'),(2,'df')

-- COMMAND ----------

drop table managed_table

-- COMMAND ----------

drop table external_tab

-- COMMAND ----------

create schema new_db

-- COMMAND ----------

describe database extended new_db

-- COMMAND ----------

show tables

-- COMMAND ----------

use demo_db;
show tables

-- COMMAND ----------


