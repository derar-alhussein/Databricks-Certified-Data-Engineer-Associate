# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying JSON 

# COMMAND ----------

dbutils.notebook.run("../Includes/Copy-Datasets")

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

spark.sql("""
          SELECT * FROM json. `${dataset.bookstore}/customers-json/export_001.json
          """)

# COMMAND ----------

spark.read.json(f"{dataset.bookstore}/customers-json/export_001.json")

# COMMAND ----------

#SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`
spark.read.json(f"{dataset_bookstore}/customers-json/export_*.json").display()


# COMMAND ----------

from pyspark.sql.functions import input_file_name
json_path = f"{dataset.bookstore}/customers-json"

df = spark.read.json(json_path).withColumn("file", input_file_name())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying text Format

# COMMAND ----------

text_path = f"{dataset.bookstore}/customers-json"
text_df = (spark.read
           .format("text")
           .load(text_path)
           .withColumn("source_file", input_file_name()))

display(text_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Querying binaryFile Format

# COMMAND ----------

binary_path = f"{dataset.bookstore}/customers-json"
binary_df = (spark.read
             .format("binary_file")
             .load(binary_path)
             .withColumn("source_file", input_file_name()))

display(binary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Querying CSV 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_csv
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limitations of Non-Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_csv

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

(spark.read
        .table("books_csv")
      .write
        .mode("append")
        .format("csv")
        .option('header', 'true')
        .option('delimiter', ';')
        .save(f"{dataset_bookstore}/books-csv"))

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS Statements

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_unparsed AS
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`;
# MAGIC
# MAGIC SELECT * FROM books_unparsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW books_tmp_vw
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE books AS
# MAGIC   SELECT * FROM books_tmp_vw;
# MAGIC   
# MAGIC SELECT * FROM books

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books
