-- Databricks notebook source
CREATE table employees
(Id INTEGER, name STRING, salary DOUBLE)

-- COMMAND ----------

insert into employees
values (3, 'Ram', 2500),(4, 'Sita', 5000)

-- COMMAND ----------

select * from employees


-- COMMAND ----------

describe detail employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json')

-- COMMAND ----------

Update employees
set salary=salary+200
where name like 'S%'

-- COMMAND ----------

select * from employees


-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees version as of 2

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------

desc history employees

-- COMMAND ----------

delete from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees version as of 3

-- COMMAND ----------

optimize employees
zorder by (id)

-- COMMAND ----------

describe history employees

-- COMMAND ----------

vacuum employees

-- COMMAND ----------


