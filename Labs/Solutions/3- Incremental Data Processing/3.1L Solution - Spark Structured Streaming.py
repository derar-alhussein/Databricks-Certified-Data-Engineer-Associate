# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Lab Solution: Spark Structured Streaming

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
# MAGIC #### Q1- Auto Loader
# MAGIC
# MAGIC Use Auto Loader to incrementally load enrollments json files from the directory **{dataset_school}/enrollments-json-raw** into a streaming view called **`enrollments_tmp_vw`**
# MAGIC

# COMMAND ----------

dataset_source = f"{dataset_school}/enrollments-json-raw"
schema_location = "dbfs:/mnt/DE-Associate/checkpoints/school/enrollments_stats"

# ANSWER
(spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schema_location)
      .load(dataset_source)
      .createOrReplaceTempView("enrollments_tmp_vw"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2 - Calculating aggregations on streaming data
# MAGIC
# MAGIC Using CTAS syntax, define a new streaming view against **`enrollments_tmp_vw`** to count the number of enrollments per **`student_id`**. Name the aggregated field: **`enrollments_counts`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC CREATE OR REPLACE TEMPORARY VIEW enrollments_per_student_tmp_vw AS
# MAGIC SELECT
# MAGIC   student_id, count(enroll_id) AS enrollments_count
# MAGIC   FROM enrollments_tmp_vw
# MAGIC   GROUP BY student_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3 - Writing stream data
# MAGIC
# MAGIC Stream the aggregated data from the **`enrollments_per_student_tmp_vw`** view to a Delta table called **`enrollments_stats`**.

# COMMAND ----------

checkpoint_path = "dbfs:/mnt/DE-Associate/checkpoints/school/enrollments_stats"

# ANSWER
query = (spark.table("enrollments_per_student_tmp_vw")
              .writeStream
              .option("checkpointLocation", checkpoint_path)
              .outputMode("complete")
              .table("enrollments_stats")
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Query the data in the **`enrollments_stats`** table to ensure data was written as expected.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_stats

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below cell to land a new json file of enrollments data

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the statistics have been updated in the table **enrollments_stats**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM enrollments_stats

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4 - Canceling streaming query
# MAGIC
# MAGIC Finally, cancel the above streaming query
