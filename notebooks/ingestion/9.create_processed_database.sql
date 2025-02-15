-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/udemyf1db/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;