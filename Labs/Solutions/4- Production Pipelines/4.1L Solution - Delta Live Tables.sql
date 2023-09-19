-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab Solution: implementing a DLT pipeline
-- MAGIC
-- MAGIC > This notebook is **not intended** to be executed interactively, but rather to be deployed as a DLT pipeline from the **workflows** tab
-- MAGIC
-- MAGIC
-- MAGIC * Help: <a href="https://docs.databricks.com/en/delta-live-tables/tutorial-sql.html" target="_blank">DLT syntax documentation</a>.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/school_schema.png" alt="School Schema" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1- Declaring Bronze Tables
-- MAGIC
-- MAGIC Declare a streaming live table, **`enrollments_bronze`**, that ingests JSON data incrementally using Auto Loader from the directory **"${datasets_path}/enrollments-json-raw"**

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE enrollments_bronze
AS SELECT * FROM cloud_files("${dataset_path}/enrollments-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Declare a live table, **`students_bronze`**, that load data directly from JSON files in the directory **"${datasets_path}/students-json"**

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH LIVE TABLE students_bronze
AS SELECT * FROM json.`${dataset_path}/students-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2 - Declaring Silver Table
-- MAGIC
-- MAGIC Declare a streaming live table, **`enrollments_cleaned`**, that:
-- MAGIC
-- MAGIC 1. Enrich the **enrollments_bronze** data through an inner join with the **`students_bronze`** table on the common **`student_id`** field to obtain the student's country
-- MAGIC 1. Implement quality control by applying a constraint to drop records with a null **`email`**
-- MAGIC 1. The table will have the following schema:
-- MAGIC
-- MAGIC | Field | Type |
-- MAGIC | --- | --- |
-- MAGIC | **`enroll_id`** | **`STRING`** |
-- MAGIC | **`total`** | **`DOUBLE`** |
-- MAGIC | **`email`** | **`STRING`** |
-- MAGIC | **`country`** | **`STRING`** |
-- MAGIC

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE enrollments_cleaned (
  CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT enroll_id, total, email, profile:address:country as country
  FROM STREAM(LIVE.enrollments_bronze) n
  LEFT JOIN LIVE.students_bronze s
    ON n.student_id = s.student_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q3- Declaring Gold Table
-- MAGIC
-- MAGIC Declare a live table, **`course_sales_per_country`** against **`enrollments_cleaned`** that calculate per **`country`** the following:
-- MAGIC * **`enrollments_count`**: the number of enrollments
-- MAGIC * **`enrollments_amount`**: the sum of the total amount of enrollments
-- MAGIC
-- MAGIC Add a comment to the table: "Course Sales Per Country"

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH LIVE TABLE course_sales_per_country
COMMENT "Course Sales Per Country"
AS
  SELECT country, count(enroll_id) AS enrollments_count, sum(total) AS enrollments_amount
  FROM LIVE.enrollments_cleaned
  GROUP BY country

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q4- Deploying DLT pipeline
-- MAGIC
-- MAGIC From the **Workflows** button on the sidebar, under the **Delta Live Tables** tab, click **Create Pipeline**
-- MAGIC
-- MAGIC Configure the pipeline settings specified below:
-- MAGIC
-- MAGIC | Setting | Instructions |
-- MAGIC |--|--|
-- MAGIC | Pipeline name | School DLT |
-- MAGIC | Product edition | Choose **Advanced** |
-- MAGIC | Pipeline mode | Choose **Triggered** |
-- MAGIC | Source code | Use the navigator to select this current notebook (4.1L - Delta Live Tables) |
-- MAGIC | Storage location | dbfs:/mnt/DE-Associate/dlt/school |
-- MAGIC | Target schema | DE_Associate_School_DLT |
-- MAGIC | Cluster policy | Leave it **None**|
-- MAGIC | Cluster mode | Choose **Fixed size**|
-- MAGIC | Workers | Enter **0**|
-- MAGIC | Photon Acceleration | Leave it unchecked |
-- MAGIC | Advanced Configuration | Click **Add Configuration** and enter:<br> - Key: **dataset_path** <br> - Value: **dbfs:/mnt/DE-Associate/datasets/school** |
-- MAGIC | Channel | Choose **Current**|
-- MAGIC
-- MAGIC Finally, click **Create**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q5 - Run your Pipeline
-- MAGIC
-- MAGIC Select **Development** mode and Click **Start** to begin the update to your pipeline's tables
