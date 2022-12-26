# Databricks notebook source
display(dbutils.fs.ls('/'))


# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------



# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('cp')

# COMMAND ----------

dbutils.data.help()

# COMMAND ----------

mydata = [('1','prasad','100k'),('2','dasari','300k')]
cols = ['id','name','salary']
df = spark.createDataFrame(mydata,cols)
df.show()
dbutils.data.summarize(df)

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID/CORD-19/2020-03-13')

dbutils.fs.head('/databricks-datasets/COVID/CORD-19/2020-03-13/all_sources_metadata_2020-03-13.csv')




# COMMAND ----------

dbutils.notebook.exit('Prasad stoping for today !!!!!')

# COMMAND ----------

dbutils.notebook.help('run')

# COMMAND ----------

var1=dbutils.notebook.run('project/myfirstnotebook',60)
print(var1)

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown(name='cars',defaultValue='Tesla',choices=['Audi','Tesla','BMW','Merc'])
print(dbutils.widgets.get('cars'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT cars1 DEFAULT 'Tesla'

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET cars1;
# MAGIC REMOVE WIDGET cars
