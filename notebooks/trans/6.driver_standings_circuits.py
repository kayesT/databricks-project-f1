# Databricks notebook source
# MAGIC %md
# MAGIC ####Produce Driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be processed

# COMMAND ----------

# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
#                             .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# race_year_list = []
# for race_year in race_results_list:
#     race_year_list.append(race_year.race_year)


# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
#                             .filter(col("race_year").isin(race_year_list))

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count, when, col
driver_standings_df = race_results_df.groupBy("race_year", "driver_name","driver_nationality", "circuit_location") \
                                     .agg(sum("points").alias("total_points"),
                                          count(when(col("position") == 1, True)).alias("wins"))   

# COMMAND ----------

#display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

driver_rank_spec = Window.partitionBy("race_year","circuit_location").orderBy(desc("total_points"),desc("wins"))

driver_standings_circuits_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %md
# MAGIC Using merge function for Delta

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name AND tgt.circuit_location = src.circuit_location"
merge_delta_data(driver_standings_circuits_df, "f1_presentation", "driver_standings_circuit", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# %sql
# SELECT rank, driver_name, total_points, wins
# FROM f1_presentation.driver_standings_circuit WHERE race_year = 2020 and circuit_location = 'Sakhir'


# COMMAND ----------

# %sql
# SELECT rank, driver_name, total_points, wins
# FROM f1_presentation.driver_standings WHERE race_year = 2020