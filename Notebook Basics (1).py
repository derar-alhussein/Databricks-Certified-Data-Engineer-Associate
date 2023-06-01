# Databricks notebook source
print('Hello world...')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "This is a message printed with SQL!" as Message

# COMMAND ----------

# MAGIC %md
# MAGIC # Header1
# MAGIC ## Header 2
# MAGIC ### Header 3

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

 
