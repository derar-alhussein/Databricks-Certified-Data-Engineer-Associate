# Databricks notebook source
df_books = spark.table("books")
display(df_books)

# COMMAND ----------

def apply_discount(price, percentage):
    return price * (1 - percentage/100)

# COMMAND ----------

apply_discount(100, 20)

# COMMAND ----------

apply_discount_udf = udf(apply_discount)

# COMMAND ----------

from pyspark.sql.functions import col, lit

df_discounts = df_books.select("price", apply_discount_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

apply_discount_py_udf = spark.udf.register("apply_discount_sql_udf", apply_discount)

# COMMAND ----------

df_discounts = df_books.select("price", apply_discount_py_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price, apply_discount_sql_udf(price, 50) AS price_after_discount
# MAGIC FROM books

# COMMAND ----------

@udf("double")
def apply_discount_decorator_udf(price, percentage):
    return price * (1 - percentage/100)

# COMMAND ----------

#apply_discount_decorator_udf(100, 20)

# COMMAND ----------

df_discounts = df_books.select("price", apply_discount_decorator_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

def vectorized_udf(price: pd.Series, percentage: pd.Series,) -> pd.Series:
    return price * (1 - percentage/100)

vectorized_udf = pandas_udf(vectorized_udf, "double")

# COMMAND ----------

@pandas_udf("double")
def vectorized_udf(price: pd.Series, percentage: pd.Series,) -> pd.Series:
    return price * (1 - percentage/100)

# COMMAND ----------

df_domains = df_books.select("price", vectorized_udf(col("price"), lit(50)))
display(df_domains)

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price, sql_vectorized_udf(price, 50) AS price_after_discount
# MAGIC FROM books

# COMMAND ----------


