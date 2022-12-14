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

CREATE TABLE alp_dasari.yeshwin1.studentexternal (id INT, name STRING, age INT) LOCATION 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/test1' ;

-- COMMAND ----------

INSERT INTO alp_dasari.yeshwin1.studentexternal VALUES   (5,'Prassd',  40), (6,'Dasari',  42);

-- COMMAND ----------

CREATE TABLE alp_dasari.yeshwin1.studentexternalcsv (id INT, name STRING, age INT) USING CSV LOCATION 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/studentcsv' ;

-- COMMAND ----------

INSERT INTO alp_dasari.yeshwin1.studentexternalcsv VALUES   (5,'Prassd',  40), (6,'Dasari',  42);

-- COMMAND ----------

SELECT * FROM alp_dasari.yeshwin1.studentexternalcsv;

-- COMMAND ----------

DESCRIBE EXTENDED alp_dasari.yeshwin1.studentexternal ;

-- COMMAND ----------

INSERT INTO alp_dasari.yeshwin1.student VALUES   (5,'Prassd',  40), (6,'Dasari',  42);


-- COMMAND ----------

show CATALOGS;

-- COMMAND ----------

USE CATALOG alp_dasari;

-- COMMAND ----------

SHOW GRANTS ON CATALOG alp_dasari;

-- COMMAND ----------

GRANT USAGE ON SCHEMA alp_dasari.yeshwin1 TO AZ-ALP-DEVELOPERS;

-- COMMAND ----------

SHOW GRANTS ON yeshwin1.student;

-- COMMAND ----------

show current schema;

-- COMMAND ----------

create database alp_dasari.managedexternal MANAGED LOCATION 'abfss://alp-gold@adlsalpeastusstorage001.dfs.core.windows.net/test1/managedexternal'; 


-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.student (id INT, name STRING, age INT) PARTITIONED BY(id) ;


-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.student  VALUES   (18,'Prassd',  40), (2,'Dasari',  41);
INSERT INTO alp_dasari.managedexternal.student  VALUES   (19,'xPdrassd',  43), (4,'xDasari',  44);

-- COMMAND ----------

OPTIMIZE alp_dasari.managedexternal.student;

-- COMMAND ----------

OPTIMIZE alp_dasari.managedexternal.student  ZORDER BY(AGE) ;

-- COMMAND ----------

VACUUM alp_dasari.managedexternal.student  RETAIN 0 HOURS ;

-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.studentexternal (id INT, name STRING, age INT) LOCATION 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/studentexternal'  PARTITIONED BY(ID) ;

-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.studentexternal  VALUES   (5,'Prassd',  40), (6,'Dasari',  42);

-- COMMAND ----------

describe formatted alp_dasari.managedexternal.studentexternal;

-- COMMAND ----------

describe formatted alp_dasari.managedexternal.student;

-- COMMAND ----------

drop table alp_dasari.managedexternal.studentexternal;

-- COMMAND ----------

drop table alp_dasari.managedexternal.student;

-- COMMAND ----------

select * from alp_dasari.managedexternal.studentexternal;

-- COMMAND ----------

SELECT * FROM alp_dasari.managedexternal.student;

-- COMMAND ----------

DELETE FROM alp_dasari.managedexternal.studentexternal;

-- COMMAND ----------

DROP DATABASE alp_dasari.managedexternal;

-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.studentexternalcsv (id INT, name STRING, age INT) USING csv LOCATION 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/studentexternalcsv'  PARTITIONED BY(ID) ;

-- COMMAND ----------

DESCRIBE TABLE FORMATTED alp_dasari.managedexternal.studentexternalcsv;

-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.studentexternalcsv  VALUES   ('Prassd',  40,3), ('Dasari',  42,5);

-- COMMAND ----------

select * from alp_dasari.managedexternal.studentexternalcsv;

-- COMMAND ----------

drop table alp_dasari.managedexternal.studentexternalcsv;

-- COMMAND ----------


