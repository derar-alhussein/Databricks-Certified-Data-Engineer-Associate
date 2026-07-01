-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

DESCRIBE TABLE customers_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Dynamic views

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_vw AS
  SELECT
    customer_id,
    CASE 
      WHEN is_account_group_member('admins_demo') THEN email
      ELSE 'REDACTED'
    END AS email,
    gender,
    CASE 
      WHEN is_account_group_member('admins_demo') THEN first_name
      ELSE 'REDACTED'
    END AS first_name,
    CASE 
      WHEN is_account_group_member('admins_demo') THEN last_name
      ELSE 'REDACTED'
    END AS last_name,
    CASE 
      WHEN is_account_group_member('admins_demo') THEN street
      ELSE 'REDACTED'
    END AS street,
    city,
    country
  FROM customers_details

-- COMMAND ----------

SELECT * FROM customers_vw

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_fr_vw AS
SELECT * FROM customers_vw
WHERE 
  CASE 
    WHEN is_account_group_member('admins_demo') THEN TRUE
    ELSE country = "France"
  END

-- COMMAND ----------

SELECT * FROM customers_fr_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Table-level row filters and column masks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1. Column masks

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('admins_demo') THEN email
            ELSE '***@***.***'
       END;

-- COMMAND ----------

ALTER TABLE customers_details ALTER COLUMN email SET MASK mask_email;

-- COMMAND ----------

SELECT * FROM customers_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.2 Row filters 

-- COMMAND ----------

CREATE OR REPLACE FUNCTION geo_filter(country STRING)
RETURN IF(is_account_group_member('admins_demo'), true, country="France");

-- COMMAND ----------

ALTER TABLE customers_details SET ROW FILTER geo_filter ON (country);

-- COMMAND ----------

SELECT * FROM customers_details

-- COMMAND ----------

ALTER TABLE customers_details ALTER COLUMN email DROP MASK;
ALTER TABLE customers_details DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Attribute-Based Access Control (ABAC) policies

-- COMMAND ----------

ALTER TABLE customers_details SET TAGS ('geo_restricted');

ALTER TABLE customers_details ALTER COLUMN country SET TAGS ('geo_restricted');

-- COMMAND ----------

ALTER TABLE customers_details ALTER COLUMN email SET TAGS ('pii' = 'email_address');

-- COMMAND ----------

CREATE OR REPLACE POLICY pii_email_masking_policy
ON SCHEMA workspace.default
COLUMN MASK workspace.default.mask_email
TO `account users` EXCEPT `admins_demo`
FOR TABLES
MATCH COLUMNS hasTagValue('pii','email_address') AS e
ON COLUMN e

-- COMMAND ----------

CREATE OR REPLACE POLICY geo_filtering_policy
ON SCHEMA workspace.default
ROW FILTER workspace.default.geo_filter
TO `account users` EXCEPT `admins_demo`
FOR TABLES
WHEN has_tag('geo_restricted')
MATCH COLUMNS has_tag('geo_restricted') AS g
USING COLUMNS (g)

-- COMMAND ----------

SELECT * FROM customers_details
