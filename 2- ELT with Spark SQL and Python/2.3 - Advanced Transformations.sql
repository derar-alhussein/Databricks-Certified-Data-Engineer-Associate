-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

-- query the first_name and adress only

-- COMMAND ----------

-- expand the profile column as a JSON struct

-- create a view parsed_customers which parses the contents of the JSON file, providing a schema

-- query the customerid, first_name and country from parsed_customers

-- COMMAND ----------

-- create a flattened view of parsed_customers, named customers_final

-- COMMAND ----------

-- expand the books column of orders, one per row

-- COMMAND ----------

-- identify all unique order_id and book_id by customer_id from orders

-- COMMAND ----------

-- flatten unique book ids into a new column

-- COMMAND ----------

-- create a view orders_enriched by joining order data with all known columns for corresponding book_id

-- COMMAND ----------

-- create a temporary view for orders_updats from orders-new

-- apply union, intersect, difference between orders and orders_new


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

-- create a table transactions, to display the number of books from 01 to 12 (one per column), in each order
