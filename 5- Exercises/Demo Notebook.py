# Databricks notebook source
# Print "Hello World"
print("Hello World")

# COMMAND ----------

a = 1
for x in range(1, 10):
    a+=1*2
    print(x + a)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls('./')

# COMMAND ----------


