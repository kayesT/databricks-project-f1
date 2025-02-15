# Databricks notebook source
# MAGIC %md 
# MAGIC ### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the Cluster (Advanced option)
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file
# MAGIC 1. We have also configed the cluster to use databricks Secret Scope

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@udemyf1db.dfs.core.windows.net"))