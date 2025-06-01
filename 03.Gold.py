# Databricks notebook source
# MAGIC %run ./lib_functions

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **File: 03.Gold**
# MAGIC
# MAGIC Este notebook é responsável por fazer as devidas agregações e extrações de métricas provenientes da camada Silver e persistidos na camada Gold, para uso em dashs ou relatórios.
# MAGIC \
# MAGIC Tem como pré-requisito a execuçao do nb `lib_functions` para carregar parâmetros e variáveis em memória, além de funções de uso neste nb.
# MAGIC \
# MAGIC Além do processamento dos dados, temos também uma query para validação do requisito proposto no desafio deste case, localizados no bloco _Validate_.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold DB

# COMMAND ----------

# Create database
create_db(g_schema, gold_path)

print(f"# DB {g_schema} criado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold processing

# COMMAND ----------

print("# Iniciando processamento...")
print("#")

# Load tables
df_location = spark.read.table(f"{s_schema}.s_location") \
                        .drop('updated_at') \
                        .alias('loc')

df_vac = spark.read.table(f"{s_schema}.s_vaccinations") \
                    .drop('updated_at') \
                    .alias('vac')

# Join tables
df_info_vac = df_vac.join(
                      df_location,
                      on=df_vac['iso_code'] == df_location['iso_code'],
                      how='left'
                    )
print("#")

# Filter and select columns
df_info_vac = df_info_vac.filter(
                            df_info_vac['vac.total_vaccinations'].isNotNull() 
                            & ~df_info_vac['vac.iso_code'].like('OWID%')
                          ) \
                    .drop('loc.iso_code')

df_info_vac = df_info_vac.select(
                          'vac.country', 
                          'vac.iso_code', 
                          'vac.date', 
                          'vac.total_vaccinations', 
                          'loc.vaccines', 
                          'loc.qty_vaccines'
                        )

# Agg operations
df_info_vac = df_info_vac.withColumn("year", year(df_info_vac['vac.date'])) \
                          .groupBy('country', 'year', 'iso_code','vaccines','qty_vaccines') \
                            .agg(
                              max('total_vaccinations') \
                              .alias('total_vaccinations')
                            )
df_info_vac.persist(StorageLevel.MEMORY_AND_DISK)
print("#")

# Ranking with Window functions
win_parms = Window.partitionBy(df_info_vac.year) \
          .orderBy(
            df_info_vac.total_vaccinations.desc(), 
            df_info_vac.qty_vaccines.desc()
          )

df_info_vac = df_info_vac.withColumn("rank_top", row_number().over(win_parms))

# End processing
df_final = df_info_vac.select(
                          col('rank_top'),
                          col('year'),
                          col('country'),
                          col('total_vaccinations'),
                          col('vaccines'),
                        )

df_final = df_final.withColumn("update_at", current_timestamp()) \
                    .filter(col("rank_top") <= n_top_vac)

df_info_vac.unpersist()

print("# Fim processamento")

###

df_final.show(5)

# COMMAND ----------

table = 'g_vaccinations'
path_table = f"{gold_path}{table}"

print("# Iniciando escrita da tabela Vaccinations")
print("#")

write_table(
  df_final,
  g_schema,
  table,
  path_table
)

print("#")
print("# Escrita da g_vaccinations concluida")

# COMMAND ----------

# MAGIC %md
# MAGIC **Validate**
# MAGIC
# MAGIC ➔ Include in the top 10 country vaccinations per year, all the vaccine used during the fight gainst covid, ordering the top 10, first by most vaccine used and most vaccinated in the year. ( use both files )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM db_covid_gold.g_vaccinations