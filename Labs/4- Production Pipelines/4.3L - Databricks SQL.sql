-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab: Design a Dashboard with DBSQL
-- MAGIC
-- MAGIC In this lab, we will design a dashboard in DBSQL that has 2 graphs:
-- MAGIC 1. Bar graph that shows the number of students per country
-- MAGIC 1. Line graph that shows the daily enrollments amount
-- MAGIC
-- MAGIC * Help: <a href="https://docs.databricks.com/sql/index.html#what-is-databricks-sql" target="_blank">Databricks SQL documentation</a>.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/dashboard.png" style="width: 1100">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1 - Working with queries in SQL Editor
-- MAGIC
-- MAGIC Run the the below query in **SQL Editor** in Databricks SQL, and then save it with the name **Student Counts**

-- COMMAND ----------

SELECT profile:address:country as country, count(student_id) AS students_count
FROM de_associate_school.students
GROUP BY profile:address:country
ORDER BY students_count DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2 - Creating a Bar Graph Visualization
-- MAGIC
-- MAGIC Create a bar graph that shows the number of students per country
-- MAGIC
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/bar_graph.png" style="width: 100">
-- MAGIC
-- MAGIC ##### Anwser:
-- MAGIC Steps:
-- MAGIC 1. Click the Add butoon (**+**) next to the results tab, and select **Visualization** from the dialog box
-- MAGIC 1. Select **`Bar`** for the **Visualization Type**
-- MAGIC 1. Set **`country`** for the **X Column**
-- MAGIC 1. Under **Y columns** click **Add column**, and set it to **`students_count`**
-- MAGIC 1. Click **Save**
-- MAGIC 1. Finally, set the title of the graph to **Student Counts Viz**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3 - Creating a New Dashboard
-- MAGIC
-- MAGIC Add the above graph to a new dashboard named **Students Statistics**
-- MAGIC
-- MAGIC ##### Anwser:
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Click the three vertical dots button at the top of the graph and select **Add to Dashboard**.
-- MAGIC 1. Click the **Create new dashboard** option
-- MAGIC 1. Name your dashboard **Students Statistics**
-- MAGIC 1. Click **Save**
-- MAGIC 1. With the new dashboard selected as the target, click **OK** to add your visualization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q4 - Creating a Line Plot Visualization
-- MAGIC
-- MAGIC 1. Run the the below query in a new query tab in the **SQL Editor**, and then save it with the name **Daily Sales**

-- COMMAND ----------

SELECT cast(from_unixtime(enroll_timestamp, 'yyyy-MM-dd HH:mm:ss') AS date) enroll_timestamp,
        sum(total) AS enrollments_amount
FROM de_associate_school.enrollments n
INNER JOIN de_associate_school.students s ON s.student_id = n.student_id
GROUP BY enroll_timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 2. Create a Line Plot Visualization that shows the daily enrollments amount
-- MAGIC
-- MAGIC
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/line_graph.png" style="width: 100">
-- MAGIC
-- MAGIC
-- MAGIC ##### Anwser:
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Click the **Add Visualization** button
-- MAGIC 1. Select **`Line`** for the **Visualization Type**
-- MAGIC 1. Set **`enroll_timestamp`** for the **X Column**
-- MAGIC 1. Under **Y columns** click **Add column**, and set it to **`enrollments_amount`**
-- MAGIC 1. Click **Save**
-- MAGIC 1. Finally, set the title of the graph to **Daily Sales Viz**
-- MAGIC 1. Click the three vertical dots button at the top of the graph and select **Add to Dashboard**.
-- MAGIC 1. Select the dashboard **Students Statistics** created above
-- MAGIC 1. Click **OK** to add your visualization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q5 - Review your Dashboard
-- MAGIC
-- MAGIC Open your Dashboard and refresh its underlaying data
-- MAGIC
-- MAGIC ##### Anwser:
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Click on the **Dashboards** button on left side bar
-- MAGIC 1. Find the dashboard **Students Statistics** created earlier. Click to open it
-- MAGIC 1. Click the **Refresh** button to update your dashboard
