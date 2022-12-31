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

--SHOW CURRENT SCHEMA;
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

-- COMMAND ----------

SHOW GRANTS `shallu.kolla@gmail.com` ON SCHEMA alp_dasari.SHAILAJA;

-- COMMAND ----------

GRANT USAGE ON SCHEMA alp_dasari.shailaja TO `AZ-CRP-DEVELOPERS`;

-- COMMAND ----------

GRANT USAGE ON CATALOG alp_dasari TO `AZ-CRP-DEVELOPERS`;

-- COMMAND ----------

show catalogs;
USE CATALOG alp_dasari;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE DATABASE SHAILAJA;
SHOW TABLES;

-- COMMAND ----------

SELECT * FROM VW_DEPARTMENT;

-- COMMAND ----------

GRANT SELECT ON VIEW  alp_dasari.shailaja.vw_department TO `AZ-CRP-DEVELOPERS`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("SHOW CATALOGS").show()
-- MAGIC spark.sql("USE CATALOG alp_dasari")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("USE SCHEMA shailaja")
-- MAGIC spark.sql("select * from vw_department").show()
-- MAGIC spark.sql("select * from vw_department").show()

-- COMMAND ----------

USE CATALOG alp_dasari;
show schemas;
use shailaja;

-- COMMAND ----------

SHOW GRANTS `AZ-ALP-DEVELOPERS` ON SCHEMA alp_dasari.shailaja;

-- COMMAND ----------

SHOW GRANTS `prasad.d393@outlook.com` ON SCHEMA alp_dasari.shailaja;

-- COMMAND ----------

show databases;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA alp_dasari.managedexternal;

-- COMMAND ----------

GRANT USAGE ON SCHEMA alp_dasari.managedexternal TO `AZ-ALP-DEVELOPERS`;
GRANT SELECT ON SCHEMA alp_dasari.managedexternal TO `AZ-ALP-DEVELOPERS`;
GRANT CREATE ON SCHEMA alp_dasari.managedexternal TO `AZ-ALP-DEVELOPERS`;



-- COMMAND ----------

GRANT MODIFY ON SCHEMA alp_dasari.managedexternal TO `AZ-ALP-DEVELOPERS`;

-- COMMAND ----------

use schema managedexternal;
show tables;
select * from testdasari;


-- COMMAND ----------

drop table testdasari;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from student;
select * from studentexternal;
