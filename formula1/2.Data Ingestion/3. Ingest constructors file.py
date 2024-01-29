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
# MAGIC ##2.Prepare Schema for constructors file

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType

# COMMAND ----------

constructor_schema = StructType([ StructField('constructorId',IntegerType(),False),
                                 StructField('constructorRef',StringType(),True),
                                 StructField('name',StringType(),True),
                                 StructField('nationality',StringType(),True),
                                 StructField('url',StringType(),True),

])

# COMMAND ----------

#DDL Type Schema
#constructor_schema = "constructorId int,constructorRef string,name string,nationality string,url string "

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Read constructors file with Schema from raw Container

# COMMAND ----------

constructors_df = spark.read.format('json')\
    .schema(constructor_schema)\
    .load(f'{raw_folder}/constructors.json')

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save constructors_df as database table

# COMMAND ----------

constructors_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.constructors')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Select Required column only

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_selected_df = constructors_df.select(col('constructorId').alias('constructor_Id'),
                                                  col('constructorRef').alias('constructor_Ref'),
                                                  col('name'),col('nationality'))

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.add ingestion date to data frame

# COMMAND ----------

constructors_ingestion_df = ingestion_date(constructors_selected_df)

# COMMAND ----------

display(constructors_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.add source column to df

# COMMAND ----------

from pyspark.sql.functions import  lit

# COMMAND ----------

dbutils.widgets.text('Source_name','')

var_source = dbutils.widgets.get('Source_name')

# COMMAND ----------

constructors_final_df =constructors_ingestion_df.withColumn('Source',lit(var_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Save constructors_final_df as database processed table and parquet format in adls

# COMMAND ----------

constructors_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.constructors')

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/constructors'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/constructors/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('constructors File Ingestion Succeed')