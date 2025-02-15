# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

#dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/raw

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

circuit_df = spark.read \
        .option("header", True) \
        .schema(circuit_schema) \
        .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

#circuit_df.printSchema()


# COMMAND ----------

#circuit_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Select Only the Required Columns

# COMMAND ----------

#circuit_selected_df = circuit_df.select("circuitId","circuitRef","name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

#circuit_selected_df = circuit_df.select(circuit_df.circuitId, circuit_df.circuitRef, circuit_df.name, circuit_df.location, circuit_df.country, circuit_df.lat, circuit_df.lng, circuit_df.alt)

# COMMAND ----------

#circuit_selected_df = circuit_df.select(circuit_df["circuitId"], circuit_df["circuitRef"], circuit_df["name"], 
#                                        circuit_df["location"], circuit_df["country"], circuit_df["lat"], circuit_df["lng"], circuit_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"), col("location"), 
                                        col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude") \
                                        .withColumn("data_source", lit(v_data_source)) \
                                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#circuit_renamed_df = circuit_selected_df.withColumnsRenamed({"circuitId" : "circuit_id", "circuitRef" : "circuit_ref"})

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# display(spark.read.delta(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Seccess")