# Databricks notebook source
# MAGIC %run ./lib_functions

# COMMAND ----------

# MAGIC %md
# MAGIC **File: 01.Stage**
# MAGIC
# MAGIC Arquivo responsável por fazer a ingestão dos arquivos bruto para a camada bronze. 
# MAGIC \
# MAGIC Tem como pré-requisito a execuçao do nb `lib_functions` para carregar parâmetros e variáveis em memória, além de funções de uso neste nb.
# MAGIC
# MAGIC **Arquivos de uso neste case:**
# MAGIC \
# MAGIC Github locations file = 'https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/locations.csv'
# MAGIC
# MAGIC Github vaccinations file = 'https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/vaccinations.json'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dirs

# COMMAND ----------

# Diretório temporário para download dos arquivos
if not path_exist(temp_path):
    print("# Dir inexistente, criando...")
    dbutils.fs.mkdirs(temp_path)
    print("# Diretório tmp criado.")

# Diretórios de trabalho
lst_work_dir = [raw_path, bronze_path, silver_path, gold_path]

for path in lst_work_dir:
    if not path_exist(path):
        print("# Dir inexistente, criando...")
        dbutils.fs.mkdirs(path)
        print("# Diretório criado.")
    
    print('#')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get locations.csv file

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/vaccinations/locations.csv -O /tmp/files/locations.csv

# COMMAND ----------

dbutils.fs.cp(f"{temp_path}locations.csv", raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get vaccinations.json file

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/vaccinations/vaccinations.json -O /tmp/files/vaccinations.json

# COMMAND ----------

dbutils.fs.cp(f"{temp_path}vaccinations.json", raw_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls file:/tmp/files/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/lake_covid/bronze/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database

# COMMAND ----------

create_db(b_schema, bronze_path)

print(f"# DB {b_schema} criado!")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Locations view

# COMMAND ----------

print("# Lendo registros de locations")

df_locations = spark.read.format("csv") \
                        .option("Header", "True") \
                        .option("inferSchema", "False") \
                        .option("delimiter", ",") \
                        .load(f"dbfs:{raw_path}locations.csv")

print("#")

df_locations = df_locations.withColumn('ingest_dt', current_timestamp())

print(f"# Qtd de registros lido: {df_locations.count()}")

###

df_locations.show(5)

# COMMAND ----------

table = 'b_location'
path_table = f"{bronze_path}{table}"

print(f"# Iniciando ingestão da tabela {table}")
print("#")

write_table(
  df_locations,
  b_schema,
  table,
  path_table
)

print("#")
print(f"# Ingestão da {table} concluida")

# COMMAND ----------

# %fs
# ls /FileStore/case_covid_19/bronze/b_location/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vaccintions view

# COMMAND ----------

print("# Lendo registros de vaccinations")

df_vaccinations = spark.read.format("json") \
                        .option("multiline","true") \
                        .load(f"dbfs:{raw_path}vaccinations.json")

df_vaccinations = df_vaccinations.withColumn("ingest_dt", current_timestamp())

print(f"# Qtd de registros lido: {df_vaccinations.count()}")

###

df_vaccinations.show(5)

# COMMAND ----------

table = 'b_vaccinations'
path_table = f"{bronze_path}{table}"

print("# Iniciando ingestão da tabela Vaccinations")
print("#")

write_table(
  df_vaccinations,
  b_schema,
  table,
  path_table
)

print("#")
print("# Ingestão da b_vaccinations concluida")