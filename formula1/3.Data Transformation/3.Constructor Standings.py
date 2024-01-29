# Databricks notebook source
# MAGIC %run
# MAGIC "/formula1/Includes/common_functions"

# COMMAND ----------

# MAGIC %run
# MAGIC "/formula1/Includes/Configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##1.Show all avaliable mounts and files in adls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/adlssandeep/presentation"

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Read race_results from presentation Container and select Required columns only

# COMMAND ----------

race_results = spark.read.parquet(f'{presentation_folder}/races_results/').select('race_year','team','position','points')

# COMMAND ----------

display(race_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Perform group By and aggregation oprations

# COMMAND ----------

from pyspark.sql.functions import count,when,sum,col

# COMMAND ----------

constructor_race_results = race_results.groupBy('race_year','team') \
                        . agg(sum('points').alias('total_points'),count(when( col('position') == 1,True)).alias ('wins'))

# COMMAND ----------

display(constructor_race_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.Perform window operations

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

# COMMAND ----------

Rank_window = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

# COMMAND ----------

Constructor_Standings = constructor_race_results.withColumn('Rank',rank().over(Rank_window))

# COMMAND ----------

display(Constructor_Standings)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.save Constructor_Standings df to database presentation table

# COMMAND ----------

Constructor_Standings.write.mode('overwrite').format('parquet').saveAsTable('formula1_presentation.Constructor_Standings')

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder}/constructor_standings/')
display(df)

# COMMAND ----------

