-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###1. Crate formula1 raw Database

-- COMMAND ----------

drop database if exists formula1_raw cascade;
create database if not exists formula1_raw;

-- COMMAND ----------

describe database formula1_raw

-- COMMAND ----------

show tables in formula1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2. Crate formula1 processed Database

-- COMMAND ----------

drop database if exists formula1_processed cascade;
create database formula1_processed
location '/mnt/adlssandeep/processed/' ;

-- COMMAND ----------

describe database formula1_processed

-- COMMAND ----------

show tables in formula1_processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3. Crate formula1 presentation Database

-- COMMAND ----------

drop database if exists formula1_presentation cascade;
create database if not exists formula1_presentation
location '/mnt/adlssandeep/presentation/';

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database formula1_presentation

-- COMMAND ----------

show tables in formula1_presentation

-- COMMAND ----------

