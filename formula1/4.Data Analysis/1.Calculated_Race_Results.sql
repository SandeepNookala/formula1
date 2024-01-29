-- Databricks notebook source
show databases

-- COMMAND ----------

drop table formula1_presentation.calculated_race_results 

-- COMMAND ----------

create table formula1_presentation.calculated_race_results 
using parquet
as 
select races.race_year,constructors.name as team_name,drivers.name as driver_name,results.position,results.points,11-results.position as calculated_points
from formula1_processed.results 
join formula1_processed.races on races.race_Id = results.race_Id 
join formula1_processed.drivers on drivers.driver_Id = results.driver_Id
join formula1_processed.constructors on constructors.constructor_Id = results.constructor_Id
where results.position <=10 

-- COMMAND ----------



-- COMMAND ----------

