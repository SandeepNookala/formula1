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

#Show all avaliable files in adls raw conatiner
display(dbutils.fs.ls('/mnt/adlssandeep/raw/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for Pitstops File 

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

Pitstops_Schema = StructType([StructField('raceId',IntegerType(),False),
                              StructField('driverId',IntegerType(),False),
                              StructField('stop',IntegerType(),False),
                              StructField('lap',IntegerType(),True),
                              StructField('time',StringType(),True),
                              StructField('duration',StringType(),True),
                              StructField('milliseconds',IntegerType(),True)
                      
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read Pitstops File with Schema from raw Container

# COMMAND ----------

Pitstops_df = spark.read.format('json')\
    .schema(Pitstops_Schema)\
    .option('multiline',True)\
    .load(f'{raw_folder}/pit_stops.json')

# COMMAND ----------

Pitstops_df.printSchema()

# COMMAND ----------

display(Pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save Pitstops_df as database table

# COMMAND ----------

Pitstops_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.pitstops')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.rename column name

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

Pitstops_name_df = Pitstops_df.withColumnRenamed('raceId','race_Id') \
                              .withColumnRenamed('driverId','driver_Id')\
                    

# COMMAND ----------

display(Pitstops_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.add ingestion date to data frame

# COMMAND ----------

Pitstops_ingestion_df = ingestion_date(Pitstops_name_df)

# COMMAND ----------

display(Pitstops_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.add source column to data frame

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text('Source_Name','')

var_source = dbutils.widgets.get('Source_Name')


# COMMAND ----------

Pitstops_final_df =Pitstops_ingestion_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Save Pitstops_final_df as database processed table and parquet format in adls

# COMMAND ----------

Pitstops_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.pitstops')

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/pitstops'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/pitstops')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Pitstops File Ingestion Succeed')