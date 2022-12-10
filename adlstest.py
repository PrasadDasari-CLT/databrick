# Databricks notebook source
dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.mount(
source='wasbs://alp-gold@adlsalpeastusstorage001.blob.core.windows.net',
mount_point='/mnt/alp',
extra_configs={'fs.azure.account.key.adlsalpeastusstorage001.blob.core.windows.net':'8a5mD2T/C7SO48N2u8itNj/NRWFWdhipntVZlYv8iJc/phKxB9vIgb1hwBBTbrqoaDoNMhE1KhH3+AStGt27MA=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/alp')

# COMMAND ----------

dbutils.fs.cp('/mnt/alp/','/mnt/alp/data/',recurse=True)

# COMMAND ----------

dbutils.fs.mount(
source='wasbs://alp-gold@adlsalpeastusstorage001.blob.core.windows.net/',
mount_point='/mnt/alp1',
extra_configs={'fs.azure.account.key.adlsalpeastusstorage001.blob.core.windows.net':'8a5mD2T/C7SO48N2u8itNj/NRWFWdhipntVZlYv8iJc/phKxB9vIgb1hwBBTbrqoaDoNMhE1KhH3+AStGt27MA=='}
)

# COMMAND ----------

dbutils.fs.unmount('/mnt/alp')
dbutils.fs.unmount('/mnt/alp1')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.mount(
source='wasbs://alp-gold@adlsalpeastusstorage001.blob.core.windows.net',
mount_point='/mnt/alp-secrete',
extra_configs={'fs.azure.account.key.adlsalpeastusstorage001.blob.core.windows.net':dbutils.secrets.get(scope='storagescope',key='adlsalpeastusstorage0011')} 
)

# COMMAND ----------

#spark.conf.set('spark.sql.hive.metastore.version','2.3.9')
dbs = spark.catalog.listDatabases()
print(dbs)

# COMMAND ----------

spark.conf.set('spark.hadoop.datanucleus.autoCreateSchema','true')
spark.conf.set('spark.hadoop.datanucleus.fixedDatastore','false')
spark.conf.set('fs.azure.account.auth.type.adlsalpeastusstorage001.dfs.core.windows.net',"SAS")
spark.conf.set('fs.azure.sas.token.provider.type.adlsalpeastusstorage001.dfs.core.windows.net','org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider')
spark.conf.set('fs.azure.sas.fixed.token.adlsalpeastusstorage001.dfs.core.windows.net','sv=2021-06-08&ss=bfqt&srt=sc&sp=rwdlacupyx&se=2022-12-10T05:44:00Z&st=2022-12-09T21:44:00Z&spr=https&sig=91NNM0vCZjjhKwQpElW7PVkdDtiJxgeUmQYQ9hgSQhc%3D')
df = spark.read.csv('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')

# COMMAND ----------

dbutils.fs.ls('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adlsalpeastusstorage001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsalpeastusstorage001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsalpeastusstorage001.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-12-10T06:52:13Z&st=2022-12-09T22:52:13Z&spr=https&sig=2r3BnI6xTIiXVoy3f7qwNUcdN1dJSudogXfa%2FTD0ZJE%3D")
df = spark.read.csv('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')


# COMMAND ----------

df.show()

# COMMAND ----------

df1 = spark.read.csv('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')
df1.show()

# COMMAND ----------

dbutils.fs.ls('abfss://alpcontainertest@adlsalpeastusstorage001.dfs.core.windows.net/test1/employees.csv')
