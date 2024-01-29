# Databricks notebook source
Circuits_results = dbutils.notebook.run('1. Ingest Circuits File',0,{'Source_Name' : 'ergast API'})

Circuits_results

# COMMAND ----------

Races_results = dbutils.notebook.run('2. Ingest Races File',0,{'Source_Name' : 'ergast API'})
Races_results

# COMMAND ----------

constructors_results = dbutils.notebook.run('3. Ingest constructors file',0,{'Source_Name' : 'ergast API'})
constructors_results

# COMMAND ----------

drivers_results = dbutils.notebook.run('4. Ingest drivers file',0,{'Source_Name' : 'ergast API'})
drivers_results

# COMMAND ----------

results_results = dbutils.notebook.run('5. Ingest results file',0,{'Source_Name' : 'ergast API'})
results_results

# COMMAND ----------

Pitstops_results = dbutils.notebook.run('6. Ingest Pitstops File',0,{'Source_Name' : 'ergast API'})
Pitstops_results

# COMMAND ----------

lap_times_results = dbutils.notebook.run('7. Ingest Multiple laptimes Files',0,{'Source_Name' : 'ergast API'})
lap_times_results

# COMMAND ----------

qualifying_results = dbutils.notebook.run('8. Ingest Multiple qualifying Files',0,{'Source_Name' : 'ergast API'})
qualifying_results