-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab Solution: Advanced ETL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to setup the lab environment 

-- COMMAND ----------

-- MAGIC %run ../Includes/Setup-Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1- Interacting with JSON data
-- MAGIC
-- MAGIC Review the nested data structures of the **profile** column in the **students** table created in the previous lab

-- COMMAND ----------

SELECT email, profile
FROM students

-- COMMAND ----------

DESCRIBE students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Use the appropriate syntax to access the **last_name** and **city** information from the **profile** column

-- COMMAND ----------

-- ANSWER
SELECT email, profile:last_name AS student_surname, profile:address:city AS student_city
FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2- Higher Order Functions
-- MAGIC
-- MAGIC Review the array column **courses** in the **enrollments** table created in the previous lab

-- COMMAND ----------

SELECT enroll_id, courses 
FROM enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Filter this array column to keep only course elements having subtotal greater than 40

-- COMMAND ----------

-- ANSWER
SELECT
  enroll_id,
  courses,
  FILTER (courses, i -> i.subtotal > 40) AS large_totals
FROM enrollments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3- SQL UDFs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Define a UDF function named **get_letter_grade** that takes one parameter named **gpa** of type DOUBLE. It returns the corresponding letter grade as indicated in the following table:
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC | GPA (4.0 Scale) | Grade Letter |
-- MAGIC |---------|-----------|
-- MAGIC |    3.50 - 4.0    |    A   |
-- MAGIC |    2.75 - 3.44    |    B  |
-- MAGIC |    2.0 - 2.74    |    C   |
-- MAGIC |    0.0 - 1.99    |    F   |

-- COMMAND ----------

-- ANSWER
CREATE FUNCTION get_letter_grade(gpa DOUBLE)
RETURNS STRING
RETURN CASE
          WHEN gpa >= 3.5 THEN "A"
          WHEN gpa >= 2.75 AND gpa < 3.5 THEN "B"
          WHEN gpa >= 2 AND gpa < 2.75 THEN "C"
          ELSE "F"
       END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Let's apply the above UDF on the **students** table created in the previous lab
-- MAGIC
-- MAGIC Fill in the below query to call the defined UDF on the **gpa** column 

-- COMMAND ----------

-- ANSWER
SELECT student_id, gpa, get_letter_grade(gpa) as letter_grade
FROM students

-- COMMAND ----------


