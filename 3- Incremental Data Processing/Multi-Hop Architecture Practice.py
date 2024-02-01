# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC files = dbutils.fs.ls(f"{dataset-bookstore}/orders-raw")
