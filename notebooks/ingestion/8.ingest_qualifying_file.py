# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read multiple JSON files using the spark dataframe reader API

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False), StructField("raceId", IntegerType(), False), StructField("driverId", IntegerType(), True), 
                                       StructField("constructorId", IntegerType(), True), StructField("number", IntegerType(), True), StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True), StructField("q2", StringType(), True), StructField("q3", StringType(), True),
                                    ])

# COMMAND ----------

qualifying_df = spark.read \
            .schema(qualifying_schema) \
            .option("multiline", True) \
            .json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Rename columns and add new colums

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Write output to parquet file

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Seccess")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select race_id, count(1) 
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc