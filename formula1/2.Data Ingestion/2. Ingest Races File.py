# Databricks notebook source
# MAGIC %run
# MAGIC "../Includes/Configurations"

# COMMAND ----------

# MAGIC %run
# MAGIC "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##1. Show all avaliable mounts and files in adls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adlssandeep/

# COMMAND ----------

#Show all avaliable files in adls raw conatiner
display(dbutils.fs.ls('/mnt/adlssandeep/raw/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for Races data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases
# MAGIC

# COMMAND ----------

races_schema = StructType([StructField('raceId',IntegerType(),False),
                           StructField('year',IntegerType(),True),
                           StructField('round',IntegerType(),True),
                           StructField('circuitId',IntegerType(),True),
                           StructField('name',StringType(),True),
                           StructField('date',DateType(),True),
                           StructField('time',StringType(),True),
                           StructField('url',StringType(),True)

])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Read races Data with Schema from raw Container

# COMMAND ----------

races_df = spark.read.format('csv')\
    .option('header',True)\
    .schema(races_schema)\
    .load(f'{raw_folder}/races.csv')

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save races_df as database table

# COMMAND ----------

races_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.races')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Select Required column only

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col('raceId'),col('circuitId'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Renamed Desired Column name

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed('raceId','race_Id') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('circuitId','circuit_Id')

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. add ingestion date to data frame

# COMMAND ----------

races_date_df = ingestion_date(races_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###8. concat date and time to Create new timestamp column

# COMMAND ----------

from pyspark.sql.functions import concat,to_timestamp,col,lit

# COMMAND ----------

race_timestamp_df = races_date_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),('time')),'yyyy-MM-dd HH:mm:ss' ))

# COMMAND ----------

display(race_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###9. drop unnecessary columns

# COMMAND ----------

races_drop_df = race_timestamp_df.drop('date','time','url')

# COMMAND ----------

display(races_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###10. add source columns to df

# COMMAND ----------

dbutils.widgets.text('Source_Name', '')
Source = dbutils.widgets.get('Source_Name')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_final_df = races_drop_df.withColumn('source',lit(Source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##11. Save races_final_df as database processed table and parquet format in adls

# COMMAND ----------

races_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.races')

# COMMAND ----------

# MAGIC %md
# MAGIC ###12.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/races'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###13.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/races/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Races File Ingestion Succeed')