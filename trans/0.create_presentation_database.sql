-- Databricks notebook source
DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/udemyf1db/presentation"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_presentation