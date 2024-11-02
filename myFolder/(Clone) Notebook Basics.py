# Databricks notebook source
print('Hello World!')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello World from SQL!" as myTest

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls 'databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('databricks-datasets')

# COMMAND ----------

display(files) ## Gibt Daten in eine strukturierte Form aus

# COMMAND ----------


