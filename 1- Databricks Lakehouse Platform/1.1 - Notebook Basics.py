# Databricks notebook source
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC Image from url: use `![Description](https://example.com/path/to/image.jpg)`
# MAGIC
# MAGIC Images:
# MAGIC
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %run ../Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

# Load the dataset into a Spark DataFrame
df = spark.read.csv('/databricks-datasets/adult/adult.data', header=True, inferSchema=True)

# Display the DataFrame
df.show()


# COMMAND ----------

display(df)

# COMMAND ----------

# Load the dataset into a DataFrame
df = spark.read.csv('/databricks-datasets/adult/adult.data', header=True, inferSchema=True)

# Create a temporary view
df.createOrReplaceTempView("adult_data_temp")


# COMMAND ----------

# MAGIC %md
# MAGIC Temporary views
# MAGIC Visibility: Temporary views created with createOrReplaceTempView do not appear in the Data Catalog. They are session-scoped, meaning they are only available within the same Spark session that created them.
# MAGIC Access: You can access a temporary view directly by its name within that session using either spark.sql() or SQL cells. For example:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query a temp table
# MAGIC SELECT * FROM adult_data_temp LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC permanent table

# COMMAND ----------

from pyspark.sql.functions import col

# List of current column names in your DataFrame
current_columns = df.columns

# Replace invalid characters in column names
new_columns = [c.replace(" ", "_").replace(",", "_").replace(";", "_").replace("{", "_").replace("}", "_")
               .replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
               for c in current_columns]

# Rename the columns
for current, new in zip(current_columns, new_columns):
    df = df.withColumnRenamed(current, new)

# Now save the DataFrame as a managed table in the Catalog
df.write.mode("overwrite").saveAsTable("default.adult_data")

# COMMAND ----------

from pyspark.sql.functions import col

# List of current column names in your DataFrame
current_columns = df.columns

# Replace invalid characters in column names
new_columns = [c.replace(" ", "_").replace(",", "_").replace(";", "_").replace("{", "_").replace("}", "_")
               .replace("(", "_").replace(")", "_").replace("\n", "_").replace("\t", "_").replace("=", "_")
               for c in current_columns]

# Rename the columns
for current, new in zip(current_columns, new_columns):
    df = df.withColumnRenamed(current, new)

# Save the DataFrame as a managed table in the Catalog
df.write.mode("overwrite").saveAsTable("default.adult_data2")


# COMMAND ----------

# MAGIC %md
# MAGIC External Table (points to data in DBFS without copying it):
# MAGIC '''
# MAGIC This approach creates a table that references the file directly in DBFS without moving it. You’ll find adult_data_external in the Catalog under the default database.
# MAGIC '''

# COMMAND ----------

# Read CSV file directly into a DataFrame
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("dbfs:/databricks-datasets/adult/adult.data")

# Display the DataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Workaround Solutions
# MAGIC Use Hive Metastore: Instead of Unity Catalog, you can create external tables in the Legacy Hive Metastore, which allows you to reference DBFS paths. To do this, you can specify hive_metastore as the catalog, if available.
# MAGIC

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_metastore.default.adult_data_external
    USING CSV
    OPTIONS (
        path 'dbfs:/databricks-datasets/adult/adult.data',
        header 'true',
        inferSchema 'true'
    )
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.default.adult_data_external LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT occupation, AVG(age) AS avg_age
# MAGIC FROM hive_metastore.default.adult_data_external
# MAGIC GROUP BY occupation
# MAGIC ORDER BY avg_age DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.default.adult_data_external
# MAGIC WHERE age > 30
# MAGIC ORDER BY age DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# Show the schema of the table to verify column names
spark.sql("DESCRIBE hive_metastore.default.adult_data_external").show()


# COMMAND ----------

# Load the table into a DataFrame and display the column names
df = spark.table("hive_metastore.default.adult_data_external")
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # Change column names of the data files

# COMMAND ----------

column_names = [
    "age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
    "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss",
    "hours_per_week", "native_country", "income"
]


# COMMAND ----------

# Load the CSV file and specify column names explicitly
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/databricks-datasets/adult/adult.data") \
    .toDF(*column_names)


# COMMAND ----------

# Save as a managed table in the Hive metastore
df.write.mode("overwrite").saveAsTable("hive_metastore.default.adult_data_corrected")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT occupation, AVG(age) AS avg_age
# MAGIC FROM hive_metastore.default.adult_data_corrected
# MAGIC GROUP BY occupation
# MAGIC ORDER BY avg_age DESC;
# MAGIC

# COMMAND ----------


