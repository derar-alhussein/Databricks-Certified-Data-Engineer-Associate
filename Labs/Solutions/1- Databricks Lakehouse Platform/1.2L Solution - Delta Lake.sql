-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Lab Solution: Delta Lake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a Delta Table 
-- MAGIC
-- MAGIC Run the cell below to create the **persons** Delta Table, and apply some operations on it.

-- COMMAND ----------

CREATE OR REPLACE TABLE persons
  (id INT, name STRING, age INT);

INSERT INTO persons
VALUES 
  (1, "Tom", 18),
  (2, "Kumar", 25);

INSERT INTO persons
VALUES 
  (3, "Ali", 50),
  (4, "Sandra", 35);

INSERT INTO persons
VALUES 
  (5, "Eric", 28),
  (6, "Salma", 42);

UPDATE persons 
SET age = age + 10
WHERE id = 1;

DELETE FROM persons
WHERE name = "Eric";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q1 - Querying Delta Lake table
-- MAGIC
-- MAGIC Query the data in the **persons** table using **SELECT** statement

-- COMMAND ----------

-- Answer
SELECT * FROM persons

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q2 - Checking table history
-- MAGIC
-- MAGIC Review the history of the table transactions using the **DESCRIBE HISTORY** command

-- COMMAND ----------

-- Answer
DESCRIBE HISTORY persons

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q3- Checking table metadata
-- MAGIC
-- MAGIC Review the basic metadata information of the table using the **DESCRIBE DETAIL** command

-- COMMAND ----------

--Answer
DESCRIBE DETAIL persons

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q4- Exploring table directory
-- MAGIC
-- MAGIC Explore the table directory using the **%fs** magic command.
-- MAGIC
-- MAGIC **Hint:** get the table location from the above metadata information

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/persons'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Q5- Exploring the transactions log
-- MAGIC
-- MAGIC Explore the **_delta_log** subfolder in the table directory

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/persons/_delta_log'
