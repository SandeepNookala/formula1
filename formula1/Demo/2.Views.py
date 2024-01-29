# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #views
# MAGIC
# MAGIC ###1.Local Temp Views
# MAGIC ###2.Gobal Temp Views
# MAGIC ###3.permanent views

# COMMAND ----------

# MAGIC %run
# MAGIC "/formula1/Includes/Configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #1.Local Temp Views

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###1. create views on dataframes
# MAGIC ###2. Available Within SparkSession or NoteBook 
# MAGIC ###3. we can't access from another SparkSession or NoteBook
# MAGIC ###4. access using sql Cell or python cell

# COMMAND ----------

races_results_df = spark.read.parquet(f'{presentation_folder}/race_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.1.creating local temp View on dataframe using pyspark

# COMMAND ----------

races_results_df.createOrReplaceTempView('spark_races_results_local_view')

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.2.access temp view using sql cell

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_races_results_local_view

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.3.access temp view using spark sql

# COMMAND ----------

spark.sql('select * from spark_races_results_local_view').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.4.creating local temp View Using Sql Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view sql_races_results_local_view
# MAGIC as select * from demo.race_results_python

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.5.show all views in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC show views in demo;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #2.Global Temp Views

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. create views on dataframes
# MAGIC ###2. Available for all SparkSession or NoteBooks in a same cluster
# MAGIC ###3. we can access from another SparkSession or NoteBook in a same cluster
# MAGIC ###4. access using sql Cell or python cell
# MAGIC ###5. View will register in global_temp database

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.1.creating Global temp View on dataframe using pyspark

# COMMAND ----------

races_results_df.createOrReplaceGlobalTempView('Spark_races_results_Global_view')

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.2.access temp view using sql cell

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.spark_races_results_global_view

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.3.access temp view using python cell

# COMMAND ----------

spark.sql('select * from global_temp.spark_races_results_global_view').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.4.Create Global temp view from sql table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace Global temp view sql_races_results_global_view
# MAGIC as select * from demo.race_results_python

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.5.show all Global temp views from global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %md
# MAGIC #3.permanent views

# COMMAND ----------

# MAGIC %md
# MAGIC #3.1. creating permanent views

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace view demo.race_results_view
# MAGIC as
# MAGIC select * from demo.race_results_python

# COMMAND ----------

# MAGIC %md
# MAGIC #3.2. show all views in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show views in demo