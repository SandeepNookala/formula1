# Databricks notebook source
# MAGIC %run
# MAGIC
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
# MAGIC
# MAGIC ls mnt/adlssandeep/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType

# COMMAND ----------

lap_times_schema = StructType([ StructField('raceId',IntegerType(),False),
                                StructField('driverId',IntegerType(),True),
                                StructField('lap',IntegerType(),False),
                                StructField('position',IntegerType(),False),
                                StructField('time',StringType(),False),
                                StructField('milliseconds',IntegerType(),False)

])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read lap_times File with Schema from raw Container

# COMMAND ----------

lap_times_df = spark.read.format('csv')\
    .schema(lap_times_schema)\
    .load(f'{raw_folder}/lap_times/lap_times_split*.csv')

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save lap_times_df as database table

# COMMAND ----------

lap_times_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.rename column name

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

lap_rename_df =lap_times_df.withColumnRenamed('raceId','race_Id') \
                           .withColumnRenamed('driverId','driver_Id')

# COMMAND ----------

display(lap_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.add ingestion date to data frame

# COMMAND ----------

lap_ingestion_df = ingestion_date(lap_rename_df)

# COMMAND ----------

display(lap_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.add Source Column to data frame

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text('Source_Name','')

var_source = dbutils.widgets.get('Source_Name')


# COMMAND ----------

lap_final_df =lap_ingestion_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.Save lap_final_df as database processed table and parquet format in adls

# COMMAND ----------

lap_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.laptimes')

# COMMAND ----------

# MAGIC %md
# MAGIC ###8.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/laptimes'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/laptimes')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('lap_times File Ingestion Succeed')