# Databricks notebook source
# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

# Import libs
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# Start SparkSession
spark = SparkSession.builder \
  .appName("Case Covid 19") \
  .config("spark.sql.shuffle.partitions", "200") \
  .config("spark.sql.files.maxPartitionBytes", "128MB") \
  .config("spark.sql.parquet.compress.codec", "snappy") \
  .config("spark.sql.adaptive.enabled", "true") \
  .getOrCreate()

# Set paths 
temp_path = "file:/tmp/files/"

raw_path = "/FileStore/lake_covid/raw/"
bronze_path = "/FileStore/lake_covid/bronze/"
silver_path = "/FileStore/lake_covid/silver/"
gold_path = "/FileStore/lake_covid/gold/"

# Schema
b_schema = 'db_covid_bronze' 
s_schema = 'db_covid_silver'
g_schema = 'db_covid_gold'

# Param limit
n_top_vac = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def path_exist(path):
    '''
    Função para validação de existência de diretório
    :param path: Path do diretório
    :return: Boolean validando a existência do diretório
    '''
    try:
        if dbutils.fs.ls(path):
            print("# Diretório existente")
            return True
    except Exception as e:
        if 'NotFound' in str(e):
            return False

# COMMAND ----------

def write_table(df, schema, table, path, mode='Overwrite'):
    '''
    Função para escrita de tabela delta.
    '''    
    df.write.format('delta') \
        .mode('overwrite') \
        .option('overwriteSchema', 'True') \
        .saveAsTable(f'{schema}.{table}',
            path=path
        )

# COMMAND ----------

def create_db(name, path):
    '''
    Função para criação de database.
    :param name: Noma do Database
    :param path: Caminho do Database
    '''
    spark.sql(f'''CREATE DATABASE IF NOT EXISTS {name}
                LOCATION '{path}' '''
            )