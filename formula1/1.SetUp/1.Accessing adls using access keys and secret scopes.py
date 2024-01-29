# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing Azure data lake storage using Access Keys
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.dbutils

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula-scope')

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.set spark configuration fs.azure.account.key

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

access_key = dbutils.secrets.get(scope="formula-scope",key="aceesskey")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlssandeep.dfs.core.windows.net",access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.list files in demo conatiner

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlssandeep.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.Read data from csv file

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load('abfss://demo@adlssandeep.dfs.core.windows.net/circuits.csv')

display(df)