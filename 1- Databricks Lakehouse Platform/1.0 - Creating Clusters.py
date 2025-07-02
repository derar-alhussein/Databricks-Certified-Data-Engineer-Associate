# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Creating Clusters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a Demo Cluster
# MAGIC
# MAGIC Create a cluster with the following configurations:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC |Cluster name|**Demo Cluster**|
# MAGIC |Cluster mode|**Signle node**|
# MAGIC |Runtime version|Select the Databricks runtime version 13.3 LTS|
# MAGIC |Photon Acceleration| Uncheck the option |
# MAGIC |Node type|4 cores|
# MAGIC |Auto termination|30 minutes|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1-	Navigate to the **Compute** tab in the left side bar.
# MAGIC
# MAGIC 2-	Under **All-purpose compute** tab, click **Create compute**.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/cluster_par1.png">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 3-	On top, click on the default name to change it. Name your cluster as **Demo Cluster**
# MAGIC
# MAGIC 4-	Select **Single node** cluster
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/cluster_par2.png">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 5-	Select the Databricks runtime version 13.3 LTS (Long Term Support)
# MAGIC
# MAGIC 6-	Uncheck the option for the **Use Photon Acceleration**
# MAGIC
# MAGIC 7-	Select a Node type of 4 cores
# MAGIC
# MAGIC 8-	Set the auto termination of the cluster to 30 minutes
# MAGIC
# MAGIC 9-	Lastly, click **Create compute**.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/cluster_par3.png">
# MAGIC </div>
