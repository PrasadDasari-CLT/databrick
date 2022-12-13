-- Databricks notebook source
show databases;
show current database;

-- COMMAND ----------

drop database alp_unitycatalog;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS alp_unitycatalog;

-- COMMAND ----------

describe database alp_unitycatalog;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database alp_prasad;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS alp_prasad LOCATION '/mnt/alpdb/test1/DB1';

-- COMMAND ----------

use schema default;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

GRANT CREATE TABLE ON SCHEMA pdcatalog.testschema TO `prasad.d393@outlook.com`;

-- COMMAND ----------

CREATE TABLE main.default.department
(
  deptcode   INT,
  deptname  STRING,
  location  STRING
);

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

INSERT INTO main.default.department VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

SELECT * from main.default.department;

-- COMMAND ----------

CREATE TABLE main.default.department1
(
  deptcode   INT,
  deptname  STRING,
  location  STRING
);

-- COMMAND ----------

create schema main.alp_unity1 MANAGED LOCATION 'abfss://alp-gold@adlsalpeastusstorage001.dfs.core.windows.net/test1/';

-- COMMAND ----------

create schema main.alp_unity;

-- COMMAND ----------

DESCRIBE SCHEMA MAIN.DEFAULT;

-- COMMAND ----------

DESCRIBE CATALOG MAIN;

-- COMMAND ----------

CREATE TABLE main.alp_unity.department1
(
  deptcode   INT,
  deptname  STRING,
  location  STRING
);


-- COMMAND ----------

INSERT INTO main.alp_unity.department1 VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

create catalog alp_dasari;

-- COMMAND ----------

create database alp_dasari.yeshwin1 MANAGED LOCATION 'abfss://alp-gold@adlsalpeastusstorage001.dfs.core.windows.net/test1/DB2'; 
                                                     

-- COMMAND ----------

drop database alp_dasari.yeshwin;

-- COMMAND ----------

CREATE TABLE alp_dasari.yeshwin1.student (id INT, name STRING, age INT) ;

-- COMMAND ----------

INSERT INTO alp_dasari.yeshwin1.student VALUES   (5,'Prassd',  40), (6,'Dasari',  42);

