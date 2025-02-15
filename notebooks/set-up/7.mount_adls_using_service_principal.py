# Databricks notebook source
# MAGIC %md 
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC ####   Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from Key vault
# MAGIC 1. Set Spark Config with App/Client ID, Directory/Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 2. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

# Copied from the documentation
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# Copied from the documentation
dbutils.fs.mount(
  source = "abfss://demo@dbf1dl.dfs.core.windows.net/",
  mount_point = "/mnt/dbf1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dbf1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/dbf1dl/demo/circuits.csv"))

# COMMAND ----------

# To see where the mount is
# display(dbutils.fs.mounts())