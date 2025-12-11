# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM <CATALOG>.<SCHEMA>.cn_daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Event Log

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM <CATALOG>.<SCHEMA>.event_log
