# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Accessing Azure data lake storage using SAS Token
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
# MAGIC
# MAGIC ###2.set spark configuration for sas token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

sastoken = dbutils.secrets.get(scope="formula-scope", key="saskeytoken")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlssandeep.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlssandeep.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlssandeep.dfs.core.windows.net",sastoken)

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