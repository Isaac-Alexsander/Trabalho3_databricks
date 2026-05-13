# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Notebook 04 — Modelagem Dimensional: SILVER → GOLD (Ralph Kimball)
# MAGIC
# MAGIC **Pipeline:** `SILVER` → `GOLD` (Star Schema)
# MAGIC
# MAGIC ### Tabelas criadas no GOLD:
# MAGIC | Tabela | Tipo | Grão |
# MAGIC |--------|------|------|
# MAGIC | `DIM_CLIENTE` | Dimensão | 1 linha por cliente |
# MAGIC | `DIM_VEICULO` | Dimensão | 1 linha por veículo |
# MAGIC | `DIM_COBERTURA` | Dimensão | 1 linha por tipo de cobertura |
# MAGIC | `DIM_LOCALIDADE` | Dimensão | 1 linha por município |
# MAGIC | `DIM_TEMPO` | Dimensão | 1 linha por data |
# MAGIC | `FATO_SINISTROS` | Fato ⭐ | 1 linha por sinistro |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 1. Criação do Schema GOLD

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS GOLD;
# MAGIC SELECT 'Schema GOLD OK' AS status;

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👤 2. DIM_CLIENTE

# COMMAND ----------

df_cliente   = spark.table("SILVER.cliente")
df_endereco  = spark.table("SILVER.endereco")
df_municipio = spark.table("SILVER.municipio")
df_estado    = spark.table("SILVER.estado")
df_regiao    = spark.table("SILVER.regiao")
df_telefone  = spark.table("SILVER.telefone")

df_tel = (
    df_telefone
    .filter(F.col("tipo") == "celular")
    .groupBy("id_cliente")
    .agg(F.first(F.concat(F.col("ddd"), F.lit(" "), F.col("numero"))).alias("telefone_celular"))
)

df_dim_cliente = (
    df_cliente
    .join(df_tel, "id_cliente", "left")
    .join(df_endereco.select("id_cliente","id_municipio","logradouro","bairro","cep"), "id_cliente", "left")
    .join(df_municipio.select("id_municipio","nome_municipio","id_estado"), "id_municipio", "left")
    .join(df_estado.select("id_estado","nome_estado","uf","id_regiao"), "id_estado", "left")
    .join(df_regiao.select("id_regiao","nome_regiao"), "id_regiao", "left")
    .select(
        F.col("id_cliente").alias("sk_cliente"),
        F.col("nome").alias("nome_cliente"),
        F.col("cpf"), F.col("data_nasc"), F.col("email"), F.col("sexo"),
        F.col("telefone_celular"),
        F.col("logradouro"), F.col("bairro"), F.col("cep"),
        F.col("nome_municipio").alias("municipio"),
        F.col("uf").alias("estado_uf"),
        F.col("nome_estado").alias("estado"),
        F.col("nome_regiao").alias("regiao"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

(df_dim_cliente.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.DIM_CLIENTE"))
print(f"✅ GOLD.DIM_CLIENTE: {df_dim_cliente.count()} registros")
display(df_dim_cliente.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚗 3. DIM_VEICULO

# COMMAND ----------

df_carro  = spark.table("SILVER.carro")
df_modelo = spark.table("SILVER.modelo")
df_marca  = spark.table("SILVER.marca")

df_dim_veiculo = (
    df_carro
    .join(df_modelo.select("id_modelo","nome_modelo","ano_modelo","id_marca"), "id_modelo", "left")
    .join(df_marca.select("id_marca","nome_marca"), "id_marca", "left")
    .select(
        F.col("id_carro").alias("sk_veiculo"),
        F.col("placa"), F.col("chassi"), F.col("cor"), F.col("ano_fabricacao"),
        F.col("nome_modelo").alias("modelo"),
        F.col("ano_modelo"),
        F.col("nome_marca").alias("marca"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

(df_dim_veiculo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.DIM_VEICULO"))
print(f"✅ GOLD.DIM_VEICULO: {df_dim_veiculo.count()} registros")
display(df_dim_veiculo.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛡️ 4. DIM_COBERTURA

# COMMAND ----------

df_apolice = spark.table("SILVER.apolice")

df_dim_cobertura = (
    df_apolice.select("tipo_cobertura").distinct()
    .withColumn("sk_cobertura", F.monotonically_increasing_id() + 1)
    .withColumn("descricao_cobertura",
        F.when(F.col("tipo_cobertura") == "basica",        "Cobre danos de terceiros e roubo parcial")
         .when(F.col("tipo_cobertura") == "intermediaria", "Cobre danos próprios, roubo e responsabilidade civil")
         .when(F.col("tipo_cobertura") == "completa",      "Cobertura total incluindo fenômenos naturais e assistência 24h")
         .otherwise("Cobertura não especificada"))
    .withColumn("nivel_cobertura",
        F.when(F.col("tipo_cobertura") == "basica",        1)
         .when(F.col("tipo_cobertura") == "intermediaria", 2)
         .when(F.col("tipo_cobertura") == "completa",      3)
         .otherwise(0))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .select("sk_cobertura","tipo_cobertura","descricao_cobertura","nivel_cobertura","_gold_timestamp")
)

(df_dim_cobertura.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.DIM_COBERTURA"))
print(f"✅ GOLD.DIM_COBERTURA: {df_dim_cobertura.count()} registros")
display(df_dim_cobertura)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 5. DIM_LOCALIDADE

# COMMAND ----------

df_dim_localidade = (
    df_municipio
    .join(df_estado.select("id_estado","nome_estado","uf","id_regiao"), "id_estado", "left")
    .join(df_regiao.select("id_regiao","nome_regiao"), "id_regiao", "left")
    .select(
        F.col("id_municipio").alias("sk_localidade"),
        F.col("nome_municipio").alias("municipio"),
        F.col("uf"), F.col("nome_estado").alias("estado"), F.col("nome_regiao").alias("regiao"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

(df_dim_localidade.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.DIM_LOCALIDADE"))
print(f"✅ GOLD.DIM_LOCALIDADE: {df_dim_localidade.count()} registros")
display(df_dim_localidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 6. DIM_TEMPO

# COMMAND ----------

df_sinistro = spark.table("SILVER.sinistro")

datas = (
    df_sinistro.select(F.col("data_ocorrencia").alias("data"))
    .union(df_sinistro.select(F.col("data_registro").alias("data")))
    .union(df_apolice.select(F.col("data_inicio").alias("data")))
    .union(df_apolice.select(F.col("data_fim").alias("data")))
    .distinct().dropna()
)

df_dim_tempo = (
    datas
    .withColumn("sk_tempo",        F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("data_completa",   F.col("data"))
    .withColumn("ano",             F.year(F.col("data")))
    .withColumn("trimestre",       F.quarter(F.col("data")))
    .withColumn("mes",             F.month(F.col("data")))
    .withColumn("nome_mes",        F.date_format(F.col("data"), "MMMM"))
    .withColumn("semana_ano",      F.weekofyear(F.col("data")))
    .withColumn("dia",             F.dayofmonth(F.col("data")))
    .withColumn("dia_semana",      F.dayofweek(F.col("data")))
    .withColumn("nome_dia_semana", F.date_format(F.col("data"), "EEEE"))
    .withColumn("eh_fim_semana",   F.dayofweek(F.col("data")).isin([1, 7]))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .select("sk_tempo","data_completa","ano","trimestre","mes","nome_mes",
            "semana_ano","dia","dia_semana","nome_dia_semana","eh_fim_semana","_gold_timestamp")
    .orderBy("sk_tempo")
)

(df_dim_tempo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.DIM_TEMPO"))
print(f"✅ GOLD.DIM_TEMPO: {df_dim_tempo.count()} datas únicas")
display(df_dim_tempo.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ 7. FATO_SINISTROS

# COMMAND ----------

df_dim_cob = spark.table("GOLD.DIM_COBERTURA").select("sk_cobertura","tipo_cobertura")
df_end_fato = spark.table("SILVER.endereco").select("id_cliente","id_municipio")

df_fato = (
    df_sinistro
    .join(df_apolice.select("id_apolice","id_carro","id_cliente","tipo_cobertura","valor_premio","numero_apolice"), "id_apolice", "left")
    .join(df_end_fato, "id_cliente", "left")
    .join(df_dim_cob, "tipo_cobertura", "left")
    .select(
        F.col("id_sinistro").alias("sk_sinistro"),
        F.col("id_cliente").alias("fk_cliente"),
        F.col("id_carro").alias("fk_veiculo"),
        F.col("sk_cobertura").alias("fk_cobertura"),
        F.col("id_municipio").alias("fk_localidade"),
        F.date_format(F.col("data_ocorrencia"), "yyyyMMdd").cast("int").alias("fk_tempo_ocorrencia"),
        F.date_format(F.col("data_registro"),   "yyyyMMdd").cast("int").alias("fk_tempo_registro"),
        F.col("id_apolice"), F.col("numero_apolice"),
        F.col("tipo_sinistro"), F.col("status_sinistro"),
        F.col("valor_prejuizo"), F.col("valor_premio"),
        F.when(
            F.col("valor_prejuizo").isNotNull() & (F.col("valor_premio") > 0),
            F.round(F.col("valor_prejuizo") / F.col("valor_premio"), 4)
        ).alias("indice_sinistralidade"),
        F.lit(1).alias("qtd_sinistros"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

(df_fato.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("GOLD.FATO_SINISTROS"))
print(f"✅ GOLD.FATO_SINISTROS: {df_fato.count()} registros")
display(df_fato.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 8. Consultas Analíticas (Star Schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sinistros e prejuízo por tipo
# MAGIC SELECT
# MAGIC     tipo_sinistro,
# MAGIC     COUNT(*)                      AS qtd_sinistros,
# MAGIC     SUM(valor_prejuizo)           AS total_prejuizo,
# MAGIC     ROUND(AVG(valor_prejuizo), 2) AS prejuizo_medio
# MAGIC FROM GOLD.FATO_SINISTROS
# MAGIC GROUP BY tipo_sinistro
# MAGIC ORDER BY total_prejuizo DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Star Schema: sinistros por cobertura
# MAGIC SELECT
# MAGIC     c.tipo_cobertura,
# MAGIC     c.nivel_cobertura,
# MAGIC     COUNT(f.sk_sinistro)  AS qtd_sinistros,
# MAGIC     SUM(f.valor_prejuizo) AS total_prejuizo
# MAGIC FROM GOLD.FATO_SINISTROS f
# MAGIC JOIN GOLD.DIM_COBERTURA  c ON f.fk_cobertura = c.sk_cobertura
# MAGIC GROUP BY c.tipo_cobertura, c.nivel_cobertura
# MAGIC ORDER BY c.nivel_cobertura

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top clientes com maior prejuízo
# MAGIC SELECT
# MAGIC     cl.nome_cliente,
# MAGIC     cl.estado,
# MAGIC     COUNT(f.sk_sinistro)  AS qtd_sinistros,
# MAGIC     SUM(f.valor_prejuizo) AS total_prejuizo
# MAGIC FROM GOLD.FATO_SINISTROS f
# MAGIC JOIN GOLD.DIM_CLIENTE    cl ON f.fk_cliente = cl.sk_cliente
# MAGIC GROUP BY cl.nome_cliente, cl.estado
# MAGIC ORDER BY total_prejuizo DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 9. Resumo Final — Schema GOLD

# COMMAND ----------

tabelas_gold = ["DIM_CLIENTE","DIM_VEICULO","DIM_COBERTURA","DIM_LOCALIDADE","DIM_TEMPO","FATO_SINISTROS"]
tipos = {"DIM_CLIENTE":"Dimensão","DIM_VEICULO":"Dimensão","DIM_COBERTURA":"Dimensão","DIM_LOCALIDADE":"Dimensão","DIM_TEMPO":"Dimensão","FATO_SINISTROS":"Fato ⭐"}

print("🏆 RESUMO GOLD — Star Schema (Ralph Kimball)\n")
print(f"{'Tabela':<22} {'Tipo':<12} {'Registros':>10}")
print("-" * 47)

for t in tabelas_gold:
    total = spark.table(f"GOLD.{t}").count()
    print(f"  {t:<20} {tipos[t]:<12} {total:>10,}")

print("\n🏁 Notebook 04 finalizado — GOLD (Star Schema) pronto para analytics!")
