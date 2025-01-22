# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

# list the contents of orders-raw

# COMMAND ----------

# COMMAND ----------

# using spark api, write an autoloader from orders-raw
#   save schema and checkpoints in "dbfs:/mnt/demo/orders_checkpoint"
#   write the results to a table `orders_updates`


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# COMMAND ----------

# land some new files
load_new_data()

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# COMMAND ----------

# show history for orders_updates

# COMMAND ----------

# delete the orders_update table

# do you also need to delete checkpoints and schemas?
