-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab: implementing a ETL pipeline
-- MAGIC
-- MAGIC > This notebook is **not intended** to be executed interactively, but rather to be deployed as a ETL pipeline from the **workflows** tab

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

CREATE ____________________
AS SELECT * FROM STREAM read_files("${datasets_path}/enrollments-json-raw", 
                                    format => 'json',
                                    inferColumnTypes => true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Declare a materialized view, **`students_bronze`**, that load data directly from JSON files in the directory **"${datasets_path}/students-json"**

-- COMMAND ----------

CREATE ____________________
AS SELECT * FROM json.`${datasets_path}/students-json`

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

CREATE OR REFRESH STREAMING TABLE enrollments_cleaned
  (CONSTRAINT ____________________ ON VIOLATION ____________________ )
AS SELECT enroll_id, total, email, profile:address:country as country
  FROM ____________________ n
  INNER ____________________ s
    ON n.student_id = s.student_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q3- Declaring Gold Table
-- MAGIC
-- MAGIC Declare a materialized view, **`course_sales_per_country`** against **`enrollments_cleaned`** that calculate per **`country`** the following:
-- MAGIC * **`enrollments_count`**: the number of enrollments
-- MAGIC * **`enrollments_amount`**: the sum of the total amount of enrollments
-- MAGIC
-- MAGIC Add a comment to the table: "Course Sales Per Country"

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW course_sales_per_country
  COMMENT ____________________
AS SELECT ____________________


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q4- Deploying ETL pipeline
-- MAGIC
-- MAGIC From the **Jobs & Pipelines** button on the sidebar, click **Create** -> **ETL Pipeline**
-- MAGIC
-- MAGIC Configure the pipeline settings specified below:
-- MAGIC
-- MAGIC | Setting | Instructions |
-- MAGIC |--|--|
-- MAGIC | Pipeline name | School ETL |
-- MAGIC | Schema | DE_Associate_School_ETL |
-- MAGIC | Advanced Configuration | Click **Add Configuration** and enter:<br> - Key: **datasets_path** <br> - Value: **/Volumes/workspace/DE_Associate_School/dataset** |
-- MAGIC
-- MAGIC Finally, click **Create**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q5 - Run your Pipeline
-- MAGIC
-- MAGIC Click **Run Pipeline** to begin the update to your pipeline's tables
