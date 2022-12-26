-- Databricks notebook source
show catalogs;


-- COMMAND ----------

USE CATALOG alp_dasari;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

USE SCHEMA shailaja;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE TABLE department
(
  deptcode   INT,
  deptname  STRING,
  location  STRING
);

-- COMMAND ----------

INSERT INTO department VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

select * from department;

-- COMMAND ----------

show tables;

-- COMMAND ----------

update department set location='CHARLOTTE'

-- COMMAND ----------

DELETE FROM DEPARTMENT where DEPTCODE=50

-- COMMAND ----------

select * from department;

-- COMMAND ----------

DROP TABLE DEPARTMENT ;

-- COMMAND ----------

DESCRIBE EXTENDED department;

-- COMMAND ----------

CREATE VIEW VW_DEPARTMENT AS SELECT * FROM DEPARTMENT WHERE DEPTCODE >40;

-- COMMAND ----------

GRANT SELECT ON VIEW alp_dasari.shailaja.vw_department TO `prasad.d393@outlook.com`;

-- COMMAND ----------

DROP VIEW VW_DEPARTMENT;

-- COMMAND ----------

SELECT mask('prasaddasari') as data;

-- COMMAND ----------

show tables;

-- COMMAND ----------

DESCRIBE EXTENDED DEPARTMENT;
