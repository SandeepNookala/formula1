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
# MAGIC ls /mnt/adlssandeep/

# COMMAND ----------

#Show all avaliable files in adls raw conatiner
display(dbutils.fs.ls('/mnt/adlssandeep/raw/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for drivers file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema =  StructType([ StructField('forename',StringType(),True),
                            StructField('surname',StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType([ StructField('code',StringType(),True),
                              StructField('dob',DateType(),True),
                              StructField('driverId',IntegerType(),False),
                              StructField('driverRef',StringType(),True),
                              StructField('name',name_schema),
                              StructField('nationality',StringType(),True),
                              StructField('number',IntegerType(),True),
                              StructField('url',StringType(),True)
                            ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read drivers file with Schema from raw Container

# COMMAND ----------

drivers_df = spark.read.format('json')\
    .schema(drivers_schema)\
    .load(f'{raw_folder}/drivers.json')

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.concate Name column to data frame

# COMMAND ----------

from pyspark.sql.functions import concat_ws,concat,lit,col

# COMMAND ----------

drivers_name_df = drivers_df.withColumn('name',concat( col('name.forename'),lit(' '),col('name.surname')))

# COMMAND ----------

display(drivers_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Save circuit_df as database table

# COMMAND ----------

drivers_name_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.drivers')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Select Required column only

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

drivers_selected_df = drivers_name_df.select( col('code'),col('dob'),col('driverId').alias('driver_Id'),\
                                         col('driverRef').alias('driver_Ref'), \
                                         col('name'), col('nationality'),col('number'))

# COMMAND ----------

display(drivers_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.add ingestion date to data frame

# COMMAND ----------

drivers_ingestion_df = ingestion_date(drivers_selected_df)

# COMMAND ----------

display(drivers_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8.add source column to data frame

# COMMAND ----------

dbutils.widgets.text('Source_Name','')

var_source = dbutils.widgets.get('Source_Name')


# COMMAND ----------

drivers_final_df =drivers_ingestion_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.write constructors final df to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.drivers')

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/drivers'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###11.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/drivers')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('drivers File Ingestion Succeed')