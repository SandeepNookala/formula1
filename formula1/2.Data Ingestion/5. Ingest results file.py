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
# MAGIC ls /mnt/adlssandeep/

# COMMAND ----------

#Show all avaliable files in adls raw conatiner
display(dbutils.fs.ls('/mnt/adlssandeep/raw/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for results file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,DoubleType

# COMMAND ----------

results_schema = StructType([ StructField('constructorId',IntegerType(),False),
                              StructField('driverId',IntegerType(),False),
                              StructField('fastestLap',IntegerType(),True),
                              StructField('fastestLapSpeed',StringType(),True),
                              StructField('fastestLapTime',StringType(),True),
                              StructField('grid',IntegerType(),True),
                              StructField('laps',IntegerType(),True),
                              StructField('milliseconds',IntegerType(),True),
                              StructField('number',IntegerType(),True),
                              StructField('points',DoubleType(),True),
                              StructField('position',IntegerType(),True),
                              StructField('positionOrder',IntegerType(),True),
                              StructField('positionText',StringType(),True),
                              StructField('raceId',IntegerType(),True),
                              StructField('rank',IntegerType(),True),
                              StructField('resultId',IntegerType(),True),
                              StructField('statusId',IntegerType(),True),
                              StructField('time',StringType(),True),
    
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read results file with Schema from raw Container

# COMMAND ----------

results_df = spark.read.format('json')\
    .schema(results_schema)\
    .load(f'{raw_folder}/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save results_df as database table

# COMMAND ----------

results_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.rename column name

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_name_df = results_df.withColumnRenamed('constructorId','constructor_Id') \
                            .withColumnRenamed('driverId','driver_Id') \
                            .withColumnRenamed('fastestLap','fastest_Lap') \
                            .withColumnRenamed('fastestLapSpeed','fastest_Lap_Speed') \
                            .withColumnRenamed('fastestLapTime','fastest_Lap_Time') \
                            .withColumnRenamed('positionOrder','position_Order') \
                            .withColumnRenamed('positionText','position_Text') \
                            .withColumnRenamed('raceId','race_Id') \
                            .withColumnRenamed('resultId','result_Id') \
                            .withColumnRenamed('statusId','status_Id')

# COMMAND ----------

display(results_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.add ingestion date to data frame

# COMMAND ----------

results_ingestion_df = ingestion_date(results_name_df)

# COMMAND ----------

display(results_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.drop statusid column

# COMMAND ----------

results_drop_df = results_ingestion_df.drop('status_Id')

# COMMAND ----------

display(results_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8.add source column to df

# COMMAND ----------

from pyspark.sql.functions import  lit

# COMMAND ----------

dbutils.widgets.text('Source_name','')

var_source = dbutils.widgets.get('Source_name')

# COMMAND ----------

results_final_df = results_drop_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##9. Save results_final_df as database processed table and parquet format in adls

# COMMAND ----------

results_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/results'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###11.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/results')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('results File Ingestion Succeed')