-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style= "colour:black;text-align:center;font-family:ariel">Report on Most Dominant drivers """
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Top 10 most Dominant Drivers

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_driver_inall_decades where driver_rank <=10 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. top 10 most Dominant Drivers over all years

-- COMMAND ----------

select race_year,
      driver_name,
      count(driver_name) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
from formula1_presentation.calculated_race_results
where driver_name in (select driver_name from formula1_presentation.Most_Dominant_driver_inall_decades where driver_rank <=10)
group by race_year,driver_name
order by race_year ,avg_points desc

-- COMMAND ----------

