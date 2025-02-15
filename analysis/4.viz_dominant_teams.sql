-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center;font-family:Ariel;">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points,
       rank() OVER (ORDER BY avg(calculated_points) DESC) AS team_rank
 FROM f1_presentation.calculated_race_results
 GROUP BY team_name
 HAVING count(1) >= 100
 ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year,
       team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
 FROM f1_presentation.calculated_race_results 
 WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5) --and race_year >= 2001
 GROUP BY race_year, team_name
 ORDER BY race_year, avg_points DESC


-- COMMAND ----------

SELECT race_year,
       team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       avg(calculated_points) AS avg_points
 FROM f1_presentation.calculated_race_results 
 WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5) --and race_year >= 2001
 GROUP BY race_year, team_name
 ORDER BY race_year, avg_points DESC