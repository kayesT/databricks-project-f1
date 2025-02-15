-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
create table IF not exists f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
Options (path "/mnt/dbf1dl/raw/circuits.csv", header "true")

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
create table IF not exists f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING
)
USING csv
Options (path "/mnt/dbf1dl/raw/races.csv", header "true")

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors tables
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING 
)
USING JSON
options (path "/mnt/dbf1dl/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
options (path "/mnt/dbf1dl/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points float,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId INT
  )
  USING JSON
  Options (path "/mnt/dbf1dl/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Pit Stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
  )
  USING JSON
  Options (path "/mnt/dbf1dl/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Lap_times table
-- MAGIC - CSV files
-- MAGIC - Multiple files in folder

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
  )
  USING CSV
  Options (path "/mnt/dbf1dl/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying table 
-- MAGIC  - JSON file
-- MAGIC  - MultiLine JSON
-- MAGIC  - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/dbf1dl/raw/qualifying", multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying