# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Mount adls using Service Principal
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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.mount

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://demo@adlssandeep.dfs.core.windows.net/",
    mount_point = "/mnt/adlssandeep/demo",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 6.display mounts

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adlssandeep/demo"))