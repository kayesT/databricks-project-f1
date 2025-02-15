-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current databse

-- COMMAND ----------

--CREATE DATABASE demo;

-- COMMAND ----------

--CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

--show databases

-- COMMAND ----------

--DESC DATABASE EXTENDED demo

-- COMMAND ----------

--SELECT current_database()

-- COMMAND ----------

--show tables

-- COMMAND ----------

--show tables IN demo


-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a manged table
-- MAGIC 1. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES FROM demo

-- COMMAND ----------

DESC EXTENDED demo.race_results_python
--DROP TABLE demo.race_results_python

-- COMMAND ----------


CREATE TABLE demo.race_results_sql
AS
select * from demo.race_results_python
WHERE race_year = 2020


-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

--show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Learning objectives
-- MAGIC 1. Create External table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an External table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap int,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "dbfs:/mnt/udemyf1db/presentation/race_results_ext_sql"

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Views on tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

 CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

show tables