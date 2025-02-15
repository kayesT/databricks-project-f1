# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregate Functions Demo

# COMMAND ----------

# MAGIC %md
# MAGIC Buit-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year==2020")

# COMMAND ----------

#display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, grouping 

# COMMAND ----------

#demo_df.select(count("*")).show()

# COMMAND ----------

#demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

#demo_df.select(sum("points")).show()

# COMMAND ----------

#demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
        .show()

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

#display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name") \
                        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                        #.sort("race_year", "total_points", ascending=False) \

# COMMAND ----------

display(demo_grouped_df.orderBy("total_points", ascending=False))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
driverRankSpec = Window.partitionBy("race_year").orderBy(demo_grouped_df["total_points"].desc())
display(demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)))

# COMMAND ----------

from pyspark.sql.functions import dense_rank
demo_grouped_df.withColumn("dense_rank", dense_rank().over(driverRankSpec)).show(100)