# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .filter("circuit_id < 70") \
                        .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
                     .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name, races_df.round)
#.filter("circuit_id in (12,24)")

# COMMAND ----------

display(race_circuit_df.select("circuit_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join

# COMMAND ----------

# Left Outer Join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# right Outer Join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                            .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# full Outer Join
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                            .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Joins

# COMMAND ----------

# left semi Join: works as inner join but returns left dataframe
race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") 

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Join
# MAGIC

# COMMAND ----------

# left anti Join: gives left dataframe which is not found on right dataframe
race_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join

# COMMAND ----------

# cross Join: cartesian product
race_circuit_df = races_df.crossJoin(circuits_df) 

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df.count()

# COMMAND ----------

races_df.count() * circuits_df.count()