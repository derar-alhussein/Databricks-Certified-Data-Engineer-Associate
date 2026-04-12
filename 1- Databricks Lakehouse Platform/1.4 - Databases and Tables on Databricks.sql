-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

-- No databricks free edition não usa o hive_metastore
-- USE CATALOG hive_metastore;

-- COMMAND ----------

-- Vou usar o catalog já criado o alfred_databricks, e não vou passar o schema bronze vou deixar ele pegar o default
USE CATALOG alfred_databricks;

CREATE OR REPLACE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

/*
❌ dbfs: ROOT/MOUNTS → BLOQUEADO Unity Catalog Free Edition
✅ Volumes Unity → /Volumes/catalog.schema.volume/
✅ Managed Tables → Storage interno automático

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

*/

-- COMMAND ----------

-- 1. Criar volume
USE CATALOG alfred_databricks;
USE SCHEMA bronze;
CREATE VOLUME IF NOT EXISTS demo_data;
SHOW VOLUMES;  -- Confirma criação

-- COMMAND ----------

-- 2. Listar o volume criado
LIST '/Volumes/alfred_databricks/bronze/demo_data/';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 3. Popular volume (Python cell)
-- MAGIC dbutils.fs.put("/Volumes/alfred_databricks/bronze/demo_data/dados.json", 
-- MAGIC   """[{"width":3,"length":2,"height":1}]""", 
-- MAGIC   True)
-- MAGIC print("Arquivo criado!")

-- COMMAND ----------

-- 4. Conferir o json criado
SELECT * FROM JSON.`/Volumes/alfred_databricks/bronze/demo_data/dados.json`;

-- COMMAND ----------

-- 5. Tabela Delta lendo JSON (suporta REPLACE)
CREATE OR REPLACE TABLE external_default_delta
USING DELTA
AS SELECT * FROM JSON.`/Volumes/alfred_databricks/bronze/demo_data/dados.json`;


-- COMMAND ----------

-- 6. Conferir a tabela delta criada 
select * from external_default_delta

-- COMMAND ----------

-- Essa seria outra alternativa sem usar o volume e o json

/*
USE CATALOG alfred_databricks;
USE SCHEMA bronze;

-- Drop se existir
DROP TABLE IF EXISTS external_default;

-- Criar Delta (suporta REPLACE)
CREATE TABLE external_default (
  width INT, 
  length INT, 
  height INT
) USING DELTA;

INSERT INTO external_default VALUES (3, 2, 1);

*/

-- COMMAND ----------

DESCRIBE EXTENDED external_default_delta;

-- COMMAND ----------

-- DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP VOLUME IF EXISTS alfred_databricks.bronze.demo_data;

-- COMMAND ----------

DROP TABLE IF EXISTS alfred_databricks.bronze.external_default_delta;

-- COMMAND ----------

DROP TABLE IF EXISTS alfred_databricks.default.managed_default;

-- COMMAND ----------

-- 1. Confirmar volume atual
SHOW VOLUMES;

-- 2. Usar contexto correto
USE CATALOG alfred_databricks;
USE SCHEMA bronze;

-- 3. Excluir
DROP VOLUME IF EXISTS demo_data;



-- COMMAND ----------

-- 4. Verificar remoção
SHOW VOLUMES;  -- Volume sumiu da lista

-- COMMAND ----------

-- LIST '/Volumes/alfred_databricks/bronze/demo_data/';

-- COMMAND ----------

USE CATALOG alfred_databricks;
USE SCHEMA bronze;

-- Excluir volume (seguro, sem erro se não existir)
DROP VOLUME IF EXISTS demo_data;

-- COMMAND ----------

DROP TABLE IF EXISTS managed_default_delta

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE IF EXISTS external_default

-- COMMAND ----------

-- %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------

CREATE OR REPLACE TABLE alfred_databricks.bronze.managed_custom
  (width INT, length INT, height INT);


-- COMMAND ----------

INSERT INTO alfred_databricks.bronze.managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

select *
from alfred_databricks.bronze.managed_custom

-- COMMAND ----------

DROP TABLE IF EXISTS alfred_databricks.bronze.managed_custom;
