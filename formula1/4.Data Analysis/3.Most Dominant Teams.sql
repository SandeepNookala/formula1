-- Databricks notebook source
select * from formula1_presentation.calculated_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1.formula1 Most Dominant Teams all decades

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_Teams_all_decades
as
select team_name, 
       sum(calculated_points) as total_points ,
       avg(calculated_points) as avg_points,
       count(team_name) as total_races,
       rank() over (order by avg(calculated_points) desc) team_rank
from formula1_presentation.calculated_race_results
group by team_name
having total_races >100
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2.formula1 Most Dominant Teams in 2000-2010

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_Teams_2000_2010
as
select team_name, 
       sum(calculated_points) as total_points ,
       avg(calculated_points) as avg_points,
       count(team_name) as total_races,
        rank() over (order by avg(calculated_points) desc) team_rank
from formula1_presentation.calculated_race_results
where race_year between 2000 and 2010
group by team_name
having total_races >100
order by avg_points desc

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_Teams_2000_2010

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3.formula1 Most Dominant Teams in 2010-2020

-- COMMAND ----------

create table formula1_presentation.Most_Dominant_Teams_2010_2020
as
select team_name, 
       sum(calculated_points) as total_points ,
       avg(calculated_points) as avg_points,
       count(team_name) as total_races,
       rank() over (order by avg(calculated_points) desc) team_rank
from formula1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by team_name
having total_races >100
order by avg_points desc

-- COMMAND ----------

select * from formula1_presentation.Most_Dominant_Teams_2010_2020