# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the CSV files using the spark dataframe reader API

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    ])

# COMMAND ----------

lap_times_df = spark.read \
            .schema(lap_times_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times*.csv")

# COMMAND ----------

#drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Rename columns and add new colums

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Write output to parquet file

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Seccess")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select race_id, count(1)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc