# Databricks notebook source
# MAGIC %md
# MAGIC ###Access dataframes using SQL
# MAGIC **Objectives**
# MAGIC
# MAGIC - Create temporary views on dataframes
# MAGIC - Access the view from SQL cell
# MAGIC - Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results Where race_year = {p_race_year}")

# COMMAND ----------

race_results_2019_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes 
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell
# MAGIC 1. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("Select * \
            from global_temp.gv_race_results").show()