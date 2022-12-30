-- Databricks notebook source
create database alp_dasari.managedexternal MANAGED LOCATION 'abfss://alp-gold@adlsalpeastusstorage001.dfs.core.windows.net/test1/managedexternal'; 

-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.student (id INT, name STRING, age INT) PARTITIONED BY(id) ;

-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.student  VALUES   (18,'Prassd',  40), (2,'Dasari',  41);
INSERT INTO alp_dasari.managedexternal.student  VALUES   (19,'xPdrassd',  43), (4,'xDasari',  44);
INSERT INTO alp_dasari.managedexternal.student  VALUES   (5,'Prassd',  40), (6,'Dasari',  42);

-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.studentexternal (id INT, name STRING, age INT) LOCATION 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/studentexternal'  PARTITIONED BY(ID) ;

-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.studentexternal  VALUES   (1,'Prassd',  41), (2,'Dasari',  42);
INSERT INTO alp_dasari.managedexternal.studentexternal  VALUES   (5,'Prassd',  43), (6,'Dasari',  44);

-- COMMAND ----------

select * from alp_dasari.managedexternal.studentexternal;

-- COMMAND ----------

select * from alp_dasari.managedexternal.student;
