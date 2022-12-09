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
