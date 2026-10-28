# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Lab: Creating a multi-task job
# MAGIC
# MAGIC In this lab, we will create a job that has 2 tasks:
# MAGIC 1. The current notebook that lands a new batch of data in the lab dataset directory
# MAGIC 1. The Delta Live Table pipeline created in the previous lab to processes this data
# MAGIC
# MAGIC * Help: <a href="https://docs.databricks.com/workflows/jobs/create-run-jobs.html" target="_blank">Databricks Jobs documentation</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/school_job.png">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1- Configuring Task 1 -  Land New Data
# MAGIC
# MAGIC
# MAGIC From the **Workflows** button on the sidebar, under the **Jobs** tab, click the **Create Job** button.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 1. Set the job name in the top-left of the screen to **School Job** 
# MAGIC 1. Configure the first task as specified below:
# MAGIC | Setting | Value |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Land New Data** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to choose the current notebook (4.2L - Jobs - Land New Data) |
# MAGIC | Cluster | Select your cluster from the dropdown, under **Existing All Purpose Clusters** |
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Click the **Create** button

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2- Configuring Task 2 - DLT pipeline
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 1. Click the add button (**+**) to add a new **Delta Live Tables pipeline** task 
# MAGIC 1. Configure the task:
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |--|--|
# MAGIC | Task name | Enter **DLT pipeline** |
# MAGIC | Type | Choose **Delta Live Tables pipeline** |
# MAGIC | Pipeline | Choose the DLT pipeline created in the previous lab |
# MAGIC | Depends on | Choose **Land New Data**, which is the previous task we defined above |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Click the **Create task** button

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3- Run the job 
# MAGIC
# MAGIC Click the **Run now** button in the top right to run this job. From the **Runs** tab, check your job run

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4- Review the finished job 
# MAGIC
# MAGIC Once all tasks completed successfully, review the contents of each task to verify its result

# COMMAND ----------

# MAGIC %md
# MAGIC > **Note**: The below cells are to be run as part of the **Task 1** to land new batch of data in the dataset directory

# COMMAND ----------

# MAGIC %run ../Includes/Setup-Lab

# COMMAND ----------

load_new_json_data()
