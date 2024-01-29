# Databricks notebook source
# MAGIC %run
# MAGIC "/formula1/Includes/common_functions"

# COMMAND ----------

# MAGIC %run
# MAGIC "/formula1/Includes/Configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. List all files in processed Container

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adlssandeep/processed/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.Read Races file from processed Container

# COMMAND ----------

races_df = spark.read.format('parquet').option('header',True).load(f'{processed_folder}/races')\
                                                             .withColumnRenamed('name','race_name') \
                                                             .withColumnRenamed('race_timestamp','race_date')
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.Read drivers file from processed Container

# COMMAND ----------

drivers_df = spark.read.format('parquet').option('header',True).load(f'{processed_folder}/drivers')\
                                                               .withColumnRenamed('name','driver_name') \
                                                               .withColumnRenamed('nationality','driver_nationality') \
                                                               .withColumnRenamed('number','driver_number')
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.Read constructors file from processed Container

# COMMAND ----------

constructors_df = spark.read.format('parquet').option('header',True).load(f'{processed_folder}/constructors') \
                                                                    .withColumnRenamed('name','team')
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.Read circuits file from processed Container

# COMMAND ----------

circuits_df = spark.read.format('parquet').option('header',True).load(f'{processed_folder}/circuits') \
                                                                .withColumnRenamed('location','circuit_location')
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.Read results file from processed Container

# COMMAND ----------

results_df = spark.read.format('parquet').option('header',True).load(f'{processed_folder}/results') \
                                                               .withColumnRenamed('time','race_time')
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Races_circuits DataFrame

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df,races_df.circuit_Id == circuits_df.circuit_Id,'inner')
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Races_Results DataFrame

# COMMAND ----------

Races_Results_df = results_df.join(races_circuits_df,races_circuits_df.race_Id == results_df.race_Id,'inner') \
                             .join(drivers_df,drivers_df.driver_Id == results_df.driver_Id,'inner' ) \
                             .join(constructors_df,constructors_df.constructor_Id ==  results_df.constructor_Id, 'inner')


display(Races_Results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8. Select Only Required Columns

# COMMAND ----------

Races_Results_select_df  = Races_Results_df.select('race_year','race_name','race_date','circuit_location',\
                                                  'driver_name','driver_number','driver_nationality','team',\
                                                 'grid','fastest_lap','race_time','points','position')
display(Races_Results_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8. add created date Column

# COMMAND ----------

Races_Results_final_df = ingestion_date(Races_Results_select_df)

# COMMAND ----------

display(Races_Results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Save Races_Results_final_df as database presentation table and parquet format in adls

# COMMAND ----------

Races_Results_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_presentation.Races_Results')

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.read data in presentation folder

# COMMAND ----------

race_results = spark.read.parquet(f'{presentation_folder}/races_results/')

display(race_results)