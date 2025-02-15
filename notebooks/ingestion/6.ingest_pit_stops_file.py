# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# %fs
# ls /mnt/udemyf1db/

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    ])

# COMMAND ----------

pit_stops_df = spark.read \
            .schema(pit_stops_schema) \
            .option("multiline", True) \
            .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Rename columns and add new colums

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Write output to parquet file

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# overwrite_partition(pit_stops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Calling merge function

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Seccess")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select race_id, count(1) 
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC Order by race_id desc