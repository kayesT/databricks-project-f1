# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Produce Constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race year for which the data is to be processed

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
constructor_standings_df = race_results_df.groupBy("race_year","team") \
                                             .agg(count(when(col("position") == 1, 1)).alias("wins"),
                                                  sum("points").alias("total_points")) 
                                                 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("wins"),desc("total_points"))
final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# overwrite_partition(final_df, "f1_presentation", "constructor_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.team = src.team"
merge_delta_data(final_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select race_year,count(1)
# MAGIC FROM f1_presentation.constructor_standings 
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC