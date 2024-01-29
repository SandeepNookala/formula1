-- Databricks notebook source
-- MAGIC %run
-- MAGIC "/formula1/Includes/Configurations"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #1.DataBase

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.1. Create New Database

-- COMMAND ----------

create  database if not exists formula1;

create database if not exists demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.2 All avaliable Databases

-- COMMAND ----------

show databases;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.3. show current Database

-- COMMAND ----------

SHOW CURRENT DATABASE;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.4. switch Database

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.5. Describe Database

-- COMMAND ----------

describe database demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.6. More details of Database

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #1.7. Drop DataBase

-- COMMAND ----------

drop database formula1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #2.Managed/Internal Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #2.1. Create Table using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #2.2. Create Table using sql

-- COMMAND ----------

create table demo.race_results_sql
as 
select * from demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #2.3. describe table

-- COMMAND ----------

describe extended demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #2.3. drop table

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #2.4. show all tables in database

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.External Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.1.create External Tables using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format('parquet').option('path','/mnt/adlssandeep/presentation/Ext_tables/race_results_external_table_python').saveAsTable('demo.race_results_external_table_python')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.2.show  create table statement

-- COMMAND ----------

show create table demo.race_results_external_table_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.3.create External Tables using sql

-- COMMAND ----------

CREATE TABLE demo.race_results_external_table_sql (
race_year INT,   
race_name STRING,   
race_date TIMESTAMP,   
circuit_location STRING,   
driver_name STRING,  
driver_number INT,  
driver_nationality STRING,   
team STRING,   
grid INT,   
fastest_lap INT,   
race_time STRING,   
points DOUBLE,   
position INT,   
ingestion_date TIMESTAMP) 
USING parquet 
LOCATION 'dbfs:/mnt/adlssandeep/presentation/Ext_tables/race_results_external_table_sql'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.4.Insert data in External Tables using sql

-- COMMAND ----------

insert into table race_results_external_table_sql select * from demo.race_results_external_table_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.5.show tables in db

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.6.describe tables

-- COMMAND ----------

describe extended race_results_external_table_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #3.7.drop tables

-- COMMAND ----------

drop table race_results_external_table_sql

-- COMMAND ----------

