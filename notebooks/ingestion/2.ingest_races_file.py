# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest races.csv file

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/raw

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_source_data = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read csv using dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Define the schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DateType,TimestampType
from pyspark.sql.functions import lit

# COMMAND ----------

df_schema = StructType(fields=[StructField("raceid", IntegerType(), False),
                               StructField("year", IntegerType(), True),
                               StructField("round", IntegerType(), True),
                               StructField("circuitid", IntegerType(), True),
                               StructField("name", StringType(), True),
                               StructField("date", StringType(), True),
                               StructField("time", StringType(), True),
                               StructField("url", StringType(), True),
                               ])

# COMMAND ----------

races_df = spark.read \
            .option("header", True) \
            .schema(df_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Rename/transform the columns as required

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, current_timestamp, lit

# COMMAND ----------

races_transformed_df = races_df.withColumnRenamed("raceid", "race_id") \
                                    .withColumnRenamed("year", "race_year") \
                                    .withColumnRenamed("circuitid", "circuit_id") \
                                    .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")) \
                                    .withColumn("data_source", lit(v_source_data)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date = add_ingestion_date(races_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Select only required columns

# COMMAND ----------

races_final_df = races_with_ingestion_date.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), \
                                            col("name"), col("ingestion_date"), col("race_timestamp"))
                                            # col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Write data to datalake as parquet

# COMMAND ----------

# races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# deleting location with python
# dbutils.fs.rm("dbfs:/mnt/udemyf1db/processed/races", True) 

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Seccess")