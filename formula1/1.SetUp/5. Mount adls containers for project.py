# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Mount adls containers for project
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
# MAGIC ### 2.Python function to pass paremeters

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
#Register Azure AD Application/Service Principle
    client_id = dbutils.secrets.get(scope="formula-scope",key="client")
    tenant_id = dbutils.secrets.get(scope="formula-scope",key="tenant")

#Generate a secret/password for application
    client_secret = dbutils.secrets.get(scope="formula-scope",key="client-secret")

#set spark config with app/client id,directory/tenant id & Secret
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
# Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
#mount adls containers
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

#display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.mount raw container

# COMMAND ----------

mount_adls('adlssandeep','raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.mount processed container

# COMMAND ----------

mount_adls('adlssandeep','processed')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###5.mount Presentation container

# COMMAND ----------

mount_adls('adlssandeep','presentation')

# COMMAND ----------

