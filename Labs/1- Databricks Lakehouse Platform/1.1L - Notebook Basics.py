# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Lab: Get started with Databricks Notebook
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1 - Renaming the Notebook
# MAGIC
# MAGIC Change the name of the current notebook to "1.1L - My first lab"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2 - Attaching a cluster
# MAGIC
# MAGIC Attach the cluster you created previously to this notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3 - Execute a Python code
# MAGIC
# MAGIC In the below cell, fill in the blank to print the result of adding the two variables x and y

# COMMAND ----------

x = 5
y = 10
result = x + y

--------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4 - Execute a SQL cell
# MAGIC
# MAGIC Change the language in the below cell to execute the SQL statement

# COMMAND ----------

_____________________

SELECT 5 + 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q5 - Create a Markdown Cell
# MAGIC
# MAGIC 1. Insert a new cell below this one
# MAGIC 1. In the new cell, add Markdown with a header and bullet points as shown in the following image
# MAGIC
# MAGIC <div>
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Labs/Includes/images/markdown.png">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q6 - Using %run command
# MAGIC
# MAGIC 1. Create a new Python notebook named **helper** in the current directory (i.e., in the **labs/1- Databricks Lakehouse Platform** folder)
# MAGIC 1. In the **helper** notebook, declare a variable called **my_country** and assign it a value of your country name. For example:
# MAGIC > my_country = "France"
# MAGIC 1. In the following cell, execute a %run command to include the **helper** notebook into this current notebook
# MAGIC > **Hint**: use a dot (**.**) to refer to the current directory

# COMMAND ----------

--------------------

# COMMAND ----------

# MAGIC %md
# MAGIC Now, run the following cell to test that you are able to print the **my_country** variable

# COMMAND ----------

print(my_country)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q7 - Functions definition
# MAGIC 1. In the **helper** notebook, define a Python function named **addition()** that print the sum of two numbers 
# MAGIC 1. Execute again the above %run command to take the function definition into account
# MAGIC 1. Run the below cell to call the function

# COMMAND ----------

num1 = 20
num2 = 30

addition(num1, num2)
