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
# MAGIC
# MAGIC ls /mnt/adlssandeep/

# COMMAND ----------

#Show all avaliable files in adls raw conatiner
display(dbutils.fs.ls('/mnt/adlssandeep/raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Prepare Schema for circuit data

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC
# MAGIC ####1.Always recommended to use Define Own Schema instead of InferSchema.
# MAGIC
# MAGIC ####2.Don't use inferSchema Option because it will create two spark jobs which not suitable for production level projects.we can use only for small amount of data or test cases
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,DoubleType,StringType

# COMMAND ----------

circuit_schema = StructType([StructField('circuitId',IntegerType(),False),
                     StructField('circuitRef',StringType(),True),
                     StructField('name',StringType(),True),
                     StructField('location',StringType(),True),
                     StructField('country',StringType(),True),
                     StructField('lat',DoubleType(),True),
                     StructField('lng',DoubleType(),True),
                     StructField('alt',IntegerType(),True),
                     StructField('url',StringType(),True)
                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Read circuit Data with Schema from raw Container

# COMMAND ----------

circuit_df = spark.read.format('csv')\
    .option('header',True)\
    .schema(circuit_schema)\
    .load(f'{raw_folder}/circuits.csv')

# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Save circuit_df as database table

# COMMAND ----------

circuit_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_raw.circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Select Required column only

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuit_df.select(col('circuitId').alias('ciruit'),col('circuitRef').alias('Ref'),col('name'))

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Add Source name
# MAGIC

# COMMAND ----------

dbutils.widgets.text('Source_Name','')
source = dbutils.widgets.get('Source_Name')

# COMMAND ----------

source

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Renamed Desired Column name

# COMMAND ----------

circuit_renamed_df = circuit_df.withColumnRenamed('circuitId','circuit_Id')\
.withColumnRenamed('circuitRef','circuit_Ref')\
.withColumnRenamed('lat','latitude')\
.withColumnRenamed('lng','longitude')\
.withColumnRenamed('alt','altitude') 

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8.add ingestion date to data frame

# COMMAND ----------

circuit_ingestion_df = ingestion_date(circuit_renamed_df)

# COMMAND ----------

display(circuit_ingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###9.add source column to df

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_source_df = circuit_ingestion_df.withColumn('source',lit(source))

# COMMAND ----------

# MAGIC %md
# MAGIC ###10.drop unnecessary columns

# COMMAND ----------

circuit_final_df = circuit_source_df.drop('url')

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##11. Save circuit_final_df as database processed table and parquet format in adls

# COMMAND ----------

circuit_final_df.write.mode('overwrite').format('parquet').saveAsTable('formula1_processed.circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC ###12.list file in processed container

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/circuits'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###13.read parquet file

# COMMAND ----------

df = spark.read.format('parquet').load(f'{processed_folder}/circuits/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Circuits File Ingestion Succeed')