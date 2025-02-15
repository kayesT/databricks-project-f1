# Databricks notebook source
# MAGIC %md 
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC ####   Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a Secret/password for the application
# MAGIC 1. Set Spark Config with App/Client ID, Directory/Tenant Id & Secret
# MAGIC 2. Assign Role "Storage Blob Contributor" to the Data Lake


client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbf1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dbf1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbf1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dbf1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dbf1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbf1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbf1dl.dfs.core.windows.net/circuits.csv"))