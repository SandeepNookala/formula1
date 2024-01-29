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
# MAGIC ##2.Prepare Schema for qualifying Files

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

qualifying_schema = StructType([ StructField('qualifyId',IntegerType(),False),
                                 StructField('raceId',IntegerType(),True),
                                 StructField('driverId',IntegerType(),True),
                                 StructField('constructorId',IntegerType(),True),
                                 StructField('number',IntegerType(),True),
                                 StructField('position',IntegerType(),True),
                                 StructField('q1',StringType(),True),
                                 StructField('q2',StringType(),True),
                                 StructField('q3',StringType(),True),
                            
])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read qualifying File with Schema from raw Container

# COMMAND ----------

qualifying_df = spark.read.format('json')\
    .schema(qualifying_schema)\
    .option('multiLine',True) \
    .load(f'{raw_folder}/qualifying/qualifying_split_*.json')

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save qualifying_df as database table

# COMMAND ----------

qualifying_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.rename column name

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

qualifying_rename_df =qualifying_df.withColumnRenamed('qualifyId','qualify_Id')\
                                   .withColumnRenamed('raceId','race_Id') \
                                   .withColumnRenamed('driverId','driver_Id') \
                                   .withColumnRenamed('constructorId','constructor_Id') 

# COMMAND ----------

display(qualifying_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.add ingestion date to data frame

# COMMAND ----------

qualifying_ingestion_df = ingestion_date(qualifying_rename_df)

# COMMAND ----------

display(qualifying_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.add source Column to data frame

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text('Source_Name','')

var_source = dbutils.widgets.get('Source_Name')

# COMMAND ----------

qualifying_final_df =qualifying_ingestion_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Save qualifying_final_df as database processed table and parquet format in adls

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/qualifying'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/qualifying')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('qualifying File Ingestion Succeed')