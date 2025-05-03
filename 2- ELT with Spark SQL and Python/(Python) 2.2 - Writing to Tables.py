# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

dbutils.notebook.run("../Includes/Copy-Datasets/")

# COMMAND ----------

spark.sql("""
          CREATE TABLE orders AS
          SELECT * FROM parquet.`${dataset.bookstore}/orders`
          """)

# COMMAND ----------

spark.sql("""
          SELECT * FROM orders
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overwriting Tables

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE TABLE orders AS
          SELECT * FROM parquet.`${dataset.bookstore}/orders`
          """)

# COMMAND ----------

spark.sql("""DESCRIBE HISTORY orders""").display()

# COMMAND ----------

spark.sql("""
          SELECT * FROM orders
          """).write.mode("overwrite").format("parquet").saveAsTable("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE orders
# MAGIC SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appending Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data

# COMMAND ----------

from delta.tables import DeltaTable

updates_path = f"{dataset.bookstore}/customers-json-new"
updates_df = spark.read.json(updates_path)
updates_df.createOrReplaceTempView('customers_updates')

customers_df = spark.read.table("customers")
updates_df = spark.read.table("customers_updates")

merge_condition = "customers.customer_id = updates.customer_id"

delta_table = DeltaTable.forName(spark,"customers")

delta_table.alias("c")\
.merge(
    source = updates_df.alias("u"),
    condition = merge_condition
)\
.whenMatchedUpdate(
    condition = "c.mail IS NULL AND u.mail IS NOT NULL",
    set = {
        "email":"u.email",
        "updated":"u.updated"
    }
)\
.whenNotMatchedInsertAll()\
.execute()
