-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Note:** If your workspace does not support the `hive_metastore` catalog, switch to the **unity-catalog** branch in this Git Folder.

-- COMMAND ----------

-- USE CATALOG hive_metastore para o databricks free edition use o codigo USE SCHEMA default ou
-- crie o seu próprio catalog + Create Catalog > NOME_DO_SEU_CATALOG, ENTAO USE CATALOG meu_catalog


-- COMMAND ----------

-- Opção 1: Se o catalog estiver vazio (sem schemas/tables)
-- DROP CATALOG IF EXISTS alfred_databricks;

-- Opção 2: Se tiver schemas/tables (exclui tudo recursivamente)
DROP CATALOG IF EXISTS alfred_databricks CASCADE;

-- COMMAND ----------

-- Criar o catalog
CREATE CATALOG IF NOT EXISTS alfred_databricks 
COMMENT 'Catalog pessoal para estudos de data engineering';

-- COMMAND ----------

USE CATALOG alfred_databricks;

-- COMMAND ----------

-- CRIAR SCHEMA BRONZE (camada raw)
CREATE SCHEMA IF NOT EXISTS bronze 
COMMENT 'Camada Bronze - dados raw';

-- COMMAND ----------

USE CATALOG alfred_databricks;
USE SCHEMA bronze;

CREATE OR REPLACE TABLE employees (
  id INT, 
  name STRING, 
  salary DOUBLE
) USING DELTA;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- 1. Ver TODAS as tabelas do catalog (information_schema)
SELECT table_name, table_schema, *
FROM system.information_schema.tables 
WHERE table_catalog = 'alfred_databricks';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O erro ocorre porque **DBFS root** (`dbfs:/user/hive/warehouse/`) está **desabilitado** na Free Edition (Unity Catalog-only) — tables gerenciadas como `employees` não ficam lá. [docs.databricks](https://docs.databricks.com/aws/en/dbfs/unity-catalog)
-- MAGIC
-- MAGIC ## Localização da Sua Tabela `employees`
-- MAGIC Tables Unity Catalog **gerenciadas** (sem LOCATION) usam storage interno gerenciado pelo Databricks — **não acessível via %fs ls** ou DBFS paths. Dados ficam em location oculta (ex: `/unity-catalog/...`). [docs.databricks](https://docs.databricks.com/aws/en/dbfs/unity-catalog)
-- MAGIC
-- MAGIC ## Comandos Corretos (Não Use DBFS)
-- MAGIC ```sql
-- MAGIC -- 1. Ver DESCRIBE da tabela (mostra location interna)
-- MAGIC DESCRIBE DETAIL alfred_databricks.default.employees;
-- MAGIC
-- MAGIC -- 2. Listar arquivos via SQL (melhor prática)
-- MAGIC LIST '/Volumes/alfred_databricks/default/employees';  -- Se usar volumes
-- MAGIC
-- MAGIC -- 3. Para arquivos raw: Crie VOLUME primeiro
-- MAGIC CREATE VOLUME IF NOT EXISTS alfred_databricks.default.raw_data;
-- MAGIC %fs ls 'dbfs:/Volumes/alfred_databricks/default/raw_data/';
-- MAGIC ```
-- MAGIC
-- MAGIC ## Solução Recomendada
-- MAGIC **Crie tabela EXTERNA** com LOCATION acessível:
-- MAGIC ```sql
-- MAGIC -- Criar volume para arquivos raw
-- MAGIC CREATE VOLUME IF NOT EXISTS alfred_databricks.default.raw_employees;
-- MAGIC
-- MAGIC -- Upload arquivo para volume via UI ou %fs
-- MAGIC %fs put /local/file.csv 'dbfs:/Volumes/alfred_databricks/default/raw_employees/';
-- MAGIC
-- MAGIC -- Tabela externa
-- MAGIC CREATE TABLE alfred_databricks.default.employees_ext
-- MAGIC USING DELTA
-- MAGIC LOCATION 'dbfs:/Volumes/alfred_databricks/default/raw_employees/';
-- MAGIC ```
-- MAGIC
-- MAGIC **Não use DBFS root** — Unity Catalog volumes são o padrão Free Edition para arquivos acessíveis. Sua tabela `employees` está segura internamente; use SQL para query! Quer migrar dados? [docs.databricks](https://docs.databricks.com/aws/en/dbfs/unity-catalog)

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- 1. Ver DESCRIBE da tabela (mostra location interna)
DESCRIBE DETAIL alfred_databricks.bronze.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'
