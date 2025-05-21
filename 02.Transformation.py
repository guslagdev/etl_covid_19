# Databricks notebook source
# MAGIC %run ./lib_functions

# COMMAND ----------

# MAGIC %md
# MAGIC **File: 02.Transformation**
# MAGIC
# MAGIC Este notebook é responsável por fazer as transformações e padronizações pertinentes com dados provenientes da camada bronze e persistindo na camada Silver.
# MAGIC \
# MAGIC Tem como pré-requisito a execuçao do nb `lib_functions` para carregar parâmetros e variáveis em memória, além de funções de uso neste nb.
# MAGIC \
# MAGIC Além do processamento dos dados, temos também queries para validação dos requisitos propostos no desafio deste case, localizados no bloco _Validate_.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver DB

# COMMAND ----------

# Create database
create_db(s_schema, silver_path)

print(f"# DB {s_schema} criado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Location

# COMMAND ----------

# Transformation
print("# Carregando dados da b_location...")
df_location = spark.read.table(f"{b_schema}.b_location")

df_location = df_location.withColumn("vaccines", split(df_location['vaccines'], ',')) \
                         .withColumn("last_observation_date", to_date(df_location['last_observation_date'], "yyyy-MM-dd")) \
                         .drop(df_location['ingest_dt'])
print("#")

df_location = df_location.withColumn("qty_vaccines", cardinality(df_location['vaccines'])) \
                         .withColumn("updated_at", current_timestamp())

print("# Fim processamento")

###

df_location.show(5)

# COMMAND ----------

table = 's_location'
path_table = f"{silver_path}{table}"

print(f"# Iniciando escrita da tabela {table}")
print("#")

write_table(
  df_location,
  s_schema,
  table,
  path_table
)

print("#")
print(f"# Escrita da {table} concluida")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Validate**
# MAGIC
# MAGIC ➔ What country(s) use more kind of vaccines (use the file locations.csv)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   location,
# MAGIC   vaccines,
# MAGIC   qty_vaccines
# MAGIC from db_covid_silver.s_location
# MAGIC order by qty_vaccines desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Vaccinations

# COMMAND ----------

# Transformation
print("# Iniciando processamento da b_vaccinations")

df_vaccinations = spark.read.table(f"{b_schema}.b_vaccinations")

print("#")

df_vaccinations = df_vaccinations.select("*", explode("data").alias("nData"))
df_vaccinations = df_vaccinations.select("*", "nData.*").drop("data", "nData", "ingest_dt")
df_vaccinations = df_vaccinations.withColumn("date", to_date(df_vaccinations['date'], "yyyy-MM-dd")) \
                                 .withColumn("updated_at", current_timestamp())

print("# Fim do processamento")

###

df_vaccinations.show(5)

# COMMAND ----------

table = 's_vaccinations'
path_table = f"{silver_path}{table}"

print("# Iniciando escrita da tabela Vaccinations")
print("#")

write_table(
  df_vaccinations,
  s_schema,
  table,
  path_table
)

print("#")
print("# Escrita da s_vaccinations concluida")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Validate**
# MAGIC
# MAGIC ➔ Top 10 country that had more vaccinations per month and year ( use the file vaccinations.json )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH count_vaccination AS (
# MAGIC   SELECT
# MAGIC     country,
# MAGIC     YEAR(`date`) AS year,
# MAGIC     MONTH(`date`) AS month,
# MAGIC     MAX(total_vaccinations) AS total_vaccinations
# MAGIC   FROM db_covid_silver.s_vaccinations
# MAGIC   WHERE iso_code NOT LIKE 'OWID%' 
# MAGIC   AND total_vaccinations IS NOT NULL
# MAGIC   GROUP BY country, year, month
# MAGIC ),
# MAGIC
# MAGIC ranking_vaccination AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_vaccinations DESC) AS rank_vaccination
# MAGIC   FROM count_vaccination
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   year,
# MAGIC   month,
# MAGIC   country,
# MAGIC   total_vaccinations, 
# MAGIC   rank_vaccination
# MAGIC FROM ranking_vaccination
# MAGIC WHERE rank_vaccination <= 10
# MAGIC ORDER BY year, month, rank_vaccination