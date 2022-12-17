-- Databricks notebook source
GRANT USAGE ON SCHEMA alp_dasari.yeshwin1 TO `AZ-ALP-DEVELOPERS`;

-- COMMAND ----------

SHOW GRANTS `prasad.d393@outlook.com` ON CATALOG alp_dasari;

-- COMMAND ----------

SHOW GRANTS `AZ-ALP-DEVELOPERS` ON CATALOG alp_dasari;

-- COMMAND ----------

GRANT USAGE ON CATALOG alp_dasari TO `AZ-ALP-DEVELOPERS`;

-- COMMAND ----------

SHOW GRANTS `AZ-ALP-DEVELOPERS` ON SCHEMA alp_dasari.YESHWIN1;

-- COMMAND ----------

CREATE SCHEMA alp_dasari.shailaja;

-- COMMAND ----------

GRANT CREATE ON SCHEMA alp_dasari.shailaja TO `AZ-ALP-DEVELOPERS`;
GRANT USAGE ON SCHEMA alp_dasari.shailaja TO `AZ-ALP-DEVELOPERS`;

-- COMMAND ----------

-- SHOW GRANTS `shallu.kolla@gmail.com` ON CATALOG alp_dasari; 
SHOW GRANTS `shallu.kolla@gmail.com` ON SCHEMA alp_dasari.SHAILAJA;


-- COMMAND ----------

SHOW CURRENT SCHEMA;
USE CATALOG ALP_DASARI;

-- COMMAND ----------

USE SHAILAJA;
SHOW CURRENT SCHEMA;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM vw_department;

-- COMMAND ----------

describe formatted vw_department;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask(x STRING) 
     RETURNS STRING 
     RETURN CONCAT(REPEAT("*",LENGTH(x)-2),RIGHT(x,2))

-- COMMAND ----------

DESCRIBE FUNCTION MASK;


-- COMMAND ----------

SELECT mask('prasaddasari') as data;

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION alp_dasari.shailaja.mask TO `AZ-ALP-DEVELOPERS`;

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

DESCRIBE EXTENDED DEPARTMENT;

-- COMMAND ----------

CREATE TABLE student (id INT, name STRING, age INT) USING CSV;
