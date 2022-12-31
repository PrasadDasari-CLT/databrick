-- Databricks notebook source
USE CATALOG ALP_DASARI;

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;


-- COMMAND ----------

drop table alp_dasari.shailaja.studentautoloader;
create table alp_dasari.shailaja.studentautoloader ( id STRING,name string,age STRING);

-- COMMAND ----------

set spark.databricks.delta.schema.autoMerge.enabled=true;
COPY INTO alp_dasari.shailaja.studentautoloader
FROM 'abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/studentcsv/'
FILEFORMAT = CSV;


-- COMMAND ----------

select * from alp_dasari.shailaja.studentautoloader;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC (
-- MAGIC spark.readStream.format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC      # The schema location directory keeps track of your data schema over time
-- MAGIC   .option("cloudFiles.schemaLocation", "abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/studentschema/")
-- MAGIC   .option("cloudFiles.schemaEvolutionMode", "none")
-- MAGIC   .option("mergeSchema", "true")
-- MAGIC   .load("abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/studentcsv/")
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", "abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/checkpoint/")
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable("alp_dasari.shailaja.studentautoloader")
-- MAGIC )

-- COMMAND ----------

SELECT * FROM cloud_files_state('abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/checkpoint/');

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC (
-- MAGIC spark.readStream.format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC      # The schema location directory keeps track of your data schema over time
-- MAGIC   .option("cloudFiles.schemaLocation", "abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/studentschema/")
-- MAGIC   .option("cloudFiles.schemaEvolutionMode", "none")
-- MAGIC   .option("cloudFiles.useIncrementalListing", "true")
-- MAGIC   .option("mergeSchema", "true")
-- MAGIC   .load("abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/studentcsv/")
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", "abfss://alp-silver@adlsalpeastusstorage001.dfs.core.windows.net/autoloader/checkpoint/")
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable("alp_dasari.shailaja.studentautoloader")
-- MAGIC     
-- MAGIC )

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC dbutils.secrets.get(scope = "alpdatabrickskeyvault", key = "adbworkspacekey")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.secrets.listScopes()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC nslookup databrickspetest1.privatelink.blob.core.windows.net
-- MAGIC nslookup databrickspetest1.dfs.core.windows.net

-- COMMAND ----------

USE CATALOG alp_dasari;

-- COMMAND ----------

USE SCHEMA managedexternal;
show tables;


-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.privatestudent (id INT, name STRING, age INT) LOCATION 'abfss://petest@databrickspetest1.privatelink.blob.core.windows.net/DB1/'

-- COMMAND ----------

CREATE TABLE alp_dasari.managedexternal.privatestudent (id INT, name STRING, age INT) LOCATION 'abfss://petest@databrickspetest1.dfs.core.windows.net/DB1/'

-- COMMAND ----------

INSERT INTO alp_dasari.managedexternal.privatestudent  VALUES   (1,'Prassd',  41), (2,'Dasari',  42);


-- COMMAND ----------

select * from alp_dasari.managedexternal.privatestudent;

-- COMMAND ----------

describe formatted  alp_dasari.managedexternal.privatestudent;
