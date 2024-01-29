-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Find most Dominant Driver in all Decades

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_driver_inall_decades
as
select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       rank() over (order by avg(calculated_points) desc) driver_rank
from formula1_presentation.calculated_race_results
group by driver_name
having total_races >=50
order by avg_points desc

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_driver_inall_decades

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Find most Dominant Driver in 2000-2010 

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_driver_in_2000_2010
as
select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       rank() over (order by avg(calculated_points) desc) driver_rank
from formula1_presentation.calculated_race_results
where race_year between 2000 and 2010
group by driver_name
having total_races >=50
order by avg_points desc

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_driver_in_2000_2010

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. Find most Dominant Driver in 2010-2020 

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_driver_in_2010_2020
as
select driver_name,
       count(driver_name) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       rank() over (order by avg(calculated_points) desc) driver_rank
from formula1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by driver_name
having total_races >=50
order by avg_points desc

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_driver_in_2010_2020

-- COMMAND ----------

