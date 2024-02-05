# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_temp AS(
# MAGIC   select customer_id, f_name, l_name, date_trunc("DD",order_timestamp) order_date, sum(quantity) books_counts
# MAGIC     from orders_silver_tmp
# MAGIC     GROUP BY customer_id, f_name, l_name, date_trunc("DD",order_timestamp)
# MAGIC )
