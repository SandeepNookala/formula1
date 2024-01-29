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

race_results = spark.read.parquet(f'{presentation_folder}/races_results/')

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Perform group By and aggregation oprations

# COMMAND ----------

driver_race_results_df = race_results.groupBy('race_year','driver_name','team','driver_nationality') \
                                 .agg(sum('points').alias('Total_Points'),count(when (col('position') == 1,True)).alias('wins'))

# COMMAND ----------

display(driver_race_results_df.filter('race_year == 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.Perform window operations

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,asc,rank

# COMMAND ----------

rank_win = Window.partitionBy('race_year').orderBy(desc('Total_Points'),desc('wins'))

# COMMAND ----------

driver_standings_df = driver_race_results_df.withColumn('Rank',rank().over(rank_win))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.save driver_standings_df to database presentation table

# COMMAND ----------

driver_standings_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_presentation.driver_standings')

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder}/driver_standings')

display(df)