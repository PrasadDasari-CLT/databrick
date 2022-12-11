-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set('fs.azure.account.auth.type.adlsalpeastusstorage001.dfs.core.windows.net',"SAS")
-- MAGIC spark.conf.set('fs.azure.sas.token.provider.type.adlsalpeastusstorage001.dfs.core.windows.net','org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider')
-- MAGIC spark.conf.set('fs.azure.sas.fixed.token.adlsalpeastusstorage001.dfs.core.windows.net','sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-12-11T11:07:01Z&st=2022-12-11T03:07:01Z&spr=https&sig=iHBm1ELd3ZMPmXVY1BiRQzTJlkp4LRoEjc23es2gM6g%3D')
-- MAGIC df = spark.read.csv('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC source='wasbs://alp-gold@adlsalpeastusstorage001.blob.core.windows.net',
-- MAGIC mount_point='/mnt/alpdb',
-- MAGIC extra_configs={'fs.azure.account.key.adlsalpeastusstorage001.blob.core.windows.net':'8a5mD2T/C7SO48N2u8itNj/NRWFWdhipntVZlYv8iJc/phKxB9vIgb1hwBBTbrqoaDoNMhE1KhH3+AStGt27MA=='}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS alp_prasad LOCATION '/mnt/alpdb/test1/DB1';

-- COMMAND ----------

use alp_prasad;
CREATE TABLE student (id INT, name STRING, age INT) USING CSV;

-- COMMAND ----------



-- COMMAND ----------

CREATE TABLE student1 as select * from student;

-- COMMAND ----------

use alp_prasad;CREATE EXTERNAL TABLE student2 (id INT, name STRING, age INT) USING CSV LOCATION '/mnt/alpdb/test1/DB1/';

-- COMMAND ----------

INSERT INTO student1 VALUES
    (5,'Prassd',  40),
    (6,'Dasari',  42);

-- COMMAND ----------

select * from student;

-- COMMAND ----------

select * from student;

-- COMMAND ----------

use alp_prasad;
select * from student1;

-- COMMAND ----------

update student1 set age = age+10 where id > 5;

-- COMMAND ----------

show databases;
show current database;

-- COMMAND ----------

select * from student1;

-- COMMAND ----------

update student set age = age+10 where id > 5;

-- COMMAND ----------

delete from student1 where age > 50

-- COMMAND ----------

select * from student1;

-- COMMAND ----------

describe history student1;

-- COMMAND ----------

SELECT * FROM STUDENT1 VERSION AS OF 0;

-- COMMAND ----------

SELECT * FROM STUDENT1 VERSION AS OF 3;

-- COMMAND ----------

OPTIMIZE STUDENT1

-- COMMAND ----------

OPTIMIZE STUDENT

-- COMMAND ----------

VACUUM STUDENT1

-- COMMAND ----------

DESCRIBE HISTORY STUDENT1;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED alp_prasad;

-- COMMAND ----------

DESCRIBE FORMATTED ALP_PRASAD.STUDENT1;

-- COMMAND ----------

DESCRIBE FORMATTED ALP_PRASAD.STUDENT;

-- COMMAND ----------

INSERT INTO ALP_PRASAD.student2 SELECT * FROM ALP_PRASAD.student1;

-- COMMAND ----------

SELECT * FROM STUDENT2;

-- COMMAND ----------

DESCRIBE FORMATTED STUDENT2;

-- COMMAND ----------

SELECT * FROM alp_prasad.STUDENT1@v02;

-- COMMAND ----------

CREATE TABLE alp_prasad.student3 (id INT, name STRING, age INT) USING CSV TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

INSERT INTO ALP_PRASAD.student3 SELECT * FROM ALP_PRASAD.student1;

-- COMMAND ----------

CREATE TABLE alp_prasad.student4 (id INT, name STRING, age INT)   TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

INSERT INTO ALP_PRASAD.student4 SELECT * FROM ALP_PRASAD.student1;

-- COMMAND ----------

SELECT * FROM ALP_PRASAD.STUDENT4;

-- COMMAND ----------

SELECT * FROM table_changes('ALP_PRASAD.STUDENT4', 0)

-- COMMAND ----------

DESCRIBE DETAIL ALP_PRASAD.STUDENT1;


-- COMMAND ----------

DESCRIBE DETAIL ALP_PRASAD.STUDENT2;


-- COMMAND ----------

VACUUM ALP_PRASAD.STUDENT1 DRY RUN;
