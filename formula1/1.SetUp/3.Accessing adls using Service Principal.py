# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Accessing Azure data lake storage using Service Principal
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
# MAGIC ###2.Register Azure AD Application/Service Principle

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula-scope",key="client")
tenant_id = dbutils.secrets.get(scope="formula-scope",key="tenant")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.Generate a secret/password for application

# COMMAND ----------

client_secret = dbutils.secrets.get(scope="formula-scope",key="client-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.set spark config with app/client id,directory/tenant id & Secret

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlssandeep.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlssandeep.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlssandeep.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.adlssandeep.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlssandeep.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. assign role 'storage Bolb Data Contributor' to the Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.list files in demo conatiner

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlssandeep.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7.Read data from csv file

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load('abfss://demo@adlssandeep.dfs.core.windows.net/circuits.csv')

display(df)