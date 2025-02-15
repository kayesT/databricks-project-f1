-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center;font-family:Ariel;">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace TEMP VIEW v_dominant_drivers
AS
Select driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points,
       rank() over (order by avg(calculated_points) desc) AS driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) --and race_year >= 2001)
group by race_year, driver_name
order by race_year,avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10 and race_year >= 2001)
group by race_year, driver_name
order by race_year,avg_points desc

-- COMMAND ----------

Select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10 and race_year >= 2001)
group by race_year, driver_name
order by race_year,avg_points desc