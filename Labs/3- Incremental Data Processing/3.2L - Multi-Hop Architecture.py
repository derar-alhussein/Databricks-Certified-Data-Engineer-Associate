# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Lab: Multi-Hop Architecture

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/school_schema.png" alt="School Schema" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to setup the lab environment

# COMMAND ----------

# MAGIC %run ../Includes/Setup-Lab

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1- Declaring Bronze Table
# MAGIC
# MAGIC Use Auto Loader to incrementally load enrollments json files from the directory **{dataset_school}/enrollments-json-raw** to a Delta table called **`bronze`**
# MAGIC

# COMMAND ----------

dataset_source = f"{dataset_school}/enrollments-json-raw"
bronze_checkpoint_path = "dbfs:/mnt/DE-Associate/checkpoints/school/bronze"
schema_location = bronze_checkpoint_path

(spark.readStream
        .___________________
        .___________________
        .___________________
        .load(dataset_source)
      .writeStream
        .___________________
        .___________________
        .table("bronze")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a streaming temporary view **bronze_tmp** from the bronze table in order to perform transformations using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .___________________("bronze_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2 - Data Cleansing & Enrichment
# MAGIC
# MAGIC Using CTAS syntax, define a new streaming view **`bronze_cleaned_tmp`** against **`bronze_tmp`** that does the following:
# MAGIC * Remove records with **quantity** of 0 item
# MAGIC * Add a column called **`processing_time`** containing the current timestamp using the **current_timestamp()** function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_cleaned_tmp AS
# MAGIC SELECT ___________________
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3 - Declaring Silver Table
# MAGIC
# MAGIC Stream the data from **`bronze_cleaned_tmp`** to a table called **`silver`**.

# COMMAND ----------

silver_checkpoint_path = "dbfs:/mnt/DE-Associate/checkpoints/school/silver"

(spark.table("bronze_cleaned_tmp")
        .___________________
        .___________________
        .___________________
        .table("silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a streaming temporary view from the silver table in order to perform business-level aggregation using SQL

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4- Declaring Gold Table
# MAGIC
# MAGIC Using CTAS syntax, define a new streaming view **`enrollments_per_student_tmp_vw`** against **`silver_tmp`** to count the number of enrollments per **`student`**. Name the aggregated field: **`enrollments_count`**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW enrollments_per_student_tmp_vw AS
# MAGIC SELECT ___________________
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Stream the aggregated data from the **`enrollments_per_student_tmp_vw`** view to a Delta table called **`gold_enrollments_stats`**.

# COMMAND ----------

gold_checkpoint_path = "dbfs:/mnt/DE-Associate/checkpoints/school/gold_enrollments_stats"

query = (spark.table("enrollments_per_student_tmp_vw")
              .writeStream
              .___________________
              .___________________
              .table("gold_enrollments_stats"))

# COMMAND ----------

# MAGIC %md
# MAGIC Query the data in the **`gold_enrollments_stats`** table to ensure data was written as expected.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_enrollments_stats

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to land new a json file of enrollments data

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Wait for the new data to be propagated, and then run the below query to verify that the statistics have been updated in the table **gold_enrollments_stats**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_enrollments_stats

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, run the below cell for canceling the above streaming queries

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
