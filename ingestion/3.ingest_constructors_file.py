# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
            .schema(constructors_schema) \
            .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

#constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Drop unwanted column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_Id") \
                                                .withColumnRenamed("constructorRef", "constructor_Ref") \
                                                .withColumn("data_source", lit(v_data_source)) \
                                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_processed.constructors

# COMMAND ----------

#%fs
#ls /mnt/udemyf1db/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Seccess")