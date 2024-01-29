-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style= "colour:black;text-align:center;font-family:ariel">Report on Most dominant teams"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Top most dominant teams

-- COMMAND ----------

select * from formula1_presentation.most_dominant_teams_all_decades

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. top 5 most Dominant team over all years

-- COMMAND ----------

select race_year,
      team_name,
      count(team_name) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from formula1_presentation.calculated_race_results
where team_name in (select team_name from formula1_presentation.most_dominant_teams_all_decades where team_rank <=5)
group by race_year,team_name
order by race_year ,avg_points desc