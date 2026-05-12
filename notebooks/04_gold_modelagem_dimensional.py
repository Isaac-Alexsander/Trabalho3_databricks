# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Notebook 04 — Modelagem Dimensional: SILVER → GOLD (Ralph Kimball)
# MAGIC
# MAGIC **Pipeline:** `SILVER` (Delta tratado) → `GOLD` (Tabelas Dimensionais — Star Schema)
# MAGIC
# MAGIC **Objetivo:** Aplicar a metodologia **Ralph Kimball** (Star Schema / Modelagem Dimensional)
# MAGIC sobre os dados da camada Silver, criando tabelas de **Dimensão** e **Fato** prontas
# MAGIC para consumo analítico por ferramentas de BI.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📐 Modelo Dimensional — Star Schema (GOLD)
# MAGIC
# MAGIC ```
# MAGIC                    ┌──────────────────┐
# MAGIC                    │   DIM_TEMPO      │
# MAGIC                    │  (data_key)      │
# MAGIC                    └────────┬─────────┘
# MAGIC                             │
# MAGIC  ┌────────────┐   ┌─────────▼──────────┐   ┌──────────────┐
# MAGIC  │ DIM_CLIENTE│───│   FATO_SINISTROS    │───│  DIM_VEICULO │
# MAGIC  └────────────┘   │                    │   └──────────────┘
# MAGIC                   │  - valor_prejuizo  │
# MAGIC  ┌────────────┐   │  - valor_premio    │   ┌──────────────┐
# MAGIC  │DIM_COBERTURA───│  - qtd_sinistros   │───│DIM_LOCALIDADE│
# MAGIC  └────────────┘   └────────────────────┘   └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Tabelas criadas no GOLD:
# MAGIC | Tabela | Tipo | Grão |
# MAGIC |--------|------|------|
# MAGIC | `DIM_CLIENTE` | Dimensão | 1 linha por cliente |
# MAGIC | `DIM_VEICULO` | Dimensão | 1 linha por veículo |
# MAGIC | `DIM_COBERTURA` | Dimensão | 1 linha por tipo de cobertura |
# MAGIC | `DIM_LOCALIDADE` | Dimensão | 1 linha por município |
# MAGIC | `DIM_TEMPO` | Dimensão | 1 linha por data |
# MAGIC | `FATO_SINISTROS` | Fato | 1 linha por sinistro registrado |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 1. Criação do Schema GOLD

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS GOLD")
print("✅ Schema GOLD criado/verificado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👤 2. DIM_CLIENTE — Dimensão de Clientes

# COMMAND ----------

from pyspark.sql import functions as F

# Leitura das tabelas Silver necessárias
df_cliente    = spark.table("SILVER.cliente")
df_endereco   = spark.table("SILVER.endereco")
df_municipio  = spark.table("SILVER.municipio")
df_estado     = spark.table("SILVER.estado")
df_regiao     = spark.table("SILVER.regiao")
df_telefone   = spark.table("SILVER.telefone")

# Agrega telefone principal (celular preferido)
df_tel_principal = (
    df_telefone
    .filter(F.col("tipo") == "celular")
    .groupBy("id_cliente")
    .agg(F.first(F.concat(F.col("ddd"), F.lit(" "), F.col("numero"))).alias("telefone_celular"))
)

# Constrói DIM_CLIENTE com endereço desnormalizado
df_dim_cliente = (
    df_cliente
    .join(df_tel_principal, "id_cliente", "left")
    .join(df_endereco.select("id_cliente", "id_municipio", "logradouro", "bairro", "cep"),
          "id_cliente", "left")
    .join(df_municipio.select("id_municipio", "nome_municipio", "id_estado"),
          "id_municipio", "left")
    .join(df_estado.select("id_estado", "nome_estado", "uf", "id_regiao"),
          "id_estado", "left")
    .join(df_regiao.select("id_regiao", "nome_regiao"),
          "id_regiao", "left")
    .select(
        # Surrogate Key
        F.col("id_cliente").alias("sk_cliente"),
        # Atributos do cliente
        F.col("nome").alias("nome_cliente"),
        F.col("cpf"),
        F.col("data_nasc"),
        F.col("email"),
        F.col("sexo"),
        F.col("telefone_celular"),
        # Localização desnormalizada
        F.col("logradouro"),
        F.col("bairro"),
        F.col("cep"),
        F.col("nome_municipio").alias("municipio"),
        F.col("uf").alias("estado_uf"),
        F.col("nome_estado").alias("estado"),
        F.col("nome_regiao").alias("regiao"),
        # Metadados
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

total_dim_cliente = df_dim_cliente.count()

(
    df_dim_cliente.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.DIM_CLIENTE")
)

print(f"✅ GOLD.DIM_CLIENTE: {total_dim_cliente} registros")
display(df_dim_cliente.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚗 3. DIM_VEICULO — Dimensão de Veículos

# COMMAND ----------

df_carro   = spark.table("SILVER.carro")
df_modelo  = spark.table("SILVER.modelo")
df_marca   = spark.table("SILVER.marca")

df_dim_veiculo = (
    df_carro
    .join(df_modelo.select("id_modelo", "nome_modelo", "ano_modelo", "id_marca"), "id_modelo", "left")
    .join(df_marca.select("id_marca", "nome_marca"),  "id_marca",  "left")
    .select(
        F.col("id_carro").alias("sk_veiculo"),
        F.col("placa"),
        F.col("chassi"),
        F.col("cor"),
        F.col("ano_fabricacao"),
        F.col("nome_modelo").alias("modelo"),
        F.col("ano_modelo"),
        F.col("nome_marca").alias("marca"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

total_dim_veiculo = df_dim_veiculo.count()

(
    df_dim_veiculo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.DIM_VEICULO")
)

print(f"✅ GOLD.DIM_VEICULO: {total_dim_veiculo} registros")
display(df_dim_veiculo.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛡️ 4. DIM_COBERTURA — Dimensão de Tipos de Cobertura

# COMMAND ----------

# Dimensão degenerada — derivada dos valores da tabela apolice
df_apolice = spark.table("SILVER.apolice")

df_dim_cobertura = (
    df_apolice
    .select("tipo_cobertura")
    .distinct()
    .withColumn("sk_cobertura", F.monotonically_increasing_id() + 1)
    .withColumn("descricao_cobertura",
        F.when(F.col("tipo_cobertura") == "basica",
               "Cobre danos de terceiros e roubo parcial")
         .when(F.col("tipo_cobertura") == "intermediaria",
               "Cobre danos próprios, roubo e responsabilidade civil")
         .when(F.col("tipo_cobertura") == "completa",
               "Cobertura total incluindo fenômenos naturais e assistência 24h")
         .otherwise("Cobertura não especificada"))
    .withColumn("nivel_cobertura",
        F.when(F.col("tipo_cobertura") == "basica",        1)
         .when(F.col("tipo_cobertura") == "intermediaria", 2)
         .when(F.col("tipo_cobertura") == "completa",      3)
         .otherwise(0))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .select("sk_cobertura", "tipo_cobertura", "descricao_cobertura",
            "nivel_cobertura", "_gold_timestamp")
)

total_dim_cobertura = df_dim_cobertura.count()

(
    df_dim_cobertura.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.DIM_COBERTURA")
)

print(f"✅ GOLD.DIM_COBERTURA: {total_dim_cobertura} registros")
display(df_dim_cobertura)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 5. DIM_LOCALIDADE — Dimensão de Localidades

# COMMAND ----------

df_municipio_s = spark.table("SILVER.municipio")
df_estado_s    = spark.table("SILVER.estado")
df_regiao_s    = spark.table("SILVER.regiao")

df_dim_localidade = (
    df_municipio_s
    .join(df_estado_s.select("id_estado", "nome_estado", "uf", "id_regiao"), "id_estado", "left")
    .join(df_regiao_s.select("id_regiao", "nome_regiao"),                     "id_regiao", "left")
    .select(
        F.col("id_municipio").alias("sk_localidade"),
        F.col("nome_municipio").alias("municipio"),
        F.col("uf"),
        F.col("nome_estado").alias("estado"),
        F.col("nome_regiao").alias("regiao"),
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

total_dim_localidade = df_dim_localidade.count()

(
    df_dim_localidade.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.DIM_LOCALIDADE")
)

print(f"✅ GOLD.DIM_LOCALIDADE: {total_dim_localidade} registros")
display(df_dim_localidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 6. DIM_TEMPO — Dimensão de Datas

# COMMAND ----------

# Gera calendário com todas as datas presentes nos sinistros e apólices
df_sinistro_s = spark.table("SILVER.sinistro")

datas_sinistro = df_sinistro_s.select(
    F.col("data_ocorrencia").alias("data")
).union(
    df_sinistro_s.select(F.col("data_registro").alias("data"))
).union(
    df_apolice.select(F.col("data_inicio").alias("data"))
).union(
    df_apolice.select(F.col("data_fim").alias("data"))
).distinct().dropna()

df_dim_tempo = (
    datas_sinistro
    .withColumn("sk_tempo",         F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("data_completa",    F.col("data"))
    .withColumn("ano",              F.year(F.col("data")))
    .withColumn("trimestre",        F.quarter(F.col("data")))
    .withColumn("mes",              F.month(F.col("data")))
    .withColumn("nome_mes",         F.date_format(F.col("data"), "MMMM"))
    .withColumn("semana_ano",       F.weekofyear(F.col("data")))
    .withColumn("dia",              F.dayofmonth(F.col("data")))
    .withColumn("dia_semana",       F.dayofweek(F.col("data")))
    .withColumn("nome_dia_semana",  F.date_format(F.col("data"), "EEEE"))
    .withColumn("eh_fim_semana",    F.dayofweek(F.col("data")).isin([1, 7]))
    .withColumn("_gold_timestamp",  F.current_timestamp())
    .select("sk_tempo", "data_completa", "ano", "trimestre", "mes", "nome_mes",
            "semana_ano", "dia", "dia_semana", "nome_dia_semana", "eh_fim_semana", "_gold_timestamp")
    .orderBy("sk_tempo")
)

total_dim_tempo = df_dim_tempo.count()

(
    df_dim_tempo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.DIM_TEMPO")
)

print(f"✅ GOLD.DIM_TEMPO: {total_dim_tempo} datas únicas")
display(df_dim_tempo.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ 7. FATO_SINISTROS — Tabela Fato Principal

# COMMAND ----------

# Joins para montar a tabela fato com todas as chaves estrangeiras

df_dim_cob_join = spark.table("GOLD.DIM_COBERTURA").select("sk_cobertura", "tipo_cobertura")

df_fato_sinistros = (
    df_sinistro_s
    # Join com apólice (traz cobertura, veículo, cliente)
    .join(df_apolice.select(
              "id_apolice", "id_carro", "id_cliente",
              "tipo_cobertura", "valor_premio", "numero_apolice"),
          "id_apolice", "left")
    # Join com endereço para localidade do cliente
    .join(df_endereco.select("id_cliente", "id_municipio"),
          "id_cliente", "left")
    # Join com DIM_COBERTURA para SK
    .join(df_dim_cob_join, "tipo_cobertura", "left")
    # Montagem das métricas e chaves
    .select(
        # Surrogate Key da Fato
        F.col("id_sinistro").alias("sk_sinistro"),
        # Chaves estrangeiras (FKs para as Dimensões)
        F.col("id_cliente").alias("fk_cliente"),
        F.col("id_carro").alias("fk_veiculo"),
        F.col("sk_cobertura").alias("fk_cobertura"),
        F.col("id_municipio").alias("fk_localidade"),
        F.date_format(F.col("data_ocorrencia"), "yyyyMMdd").cast("int").alias("fk_tempo_ocorrencia"),
        F.date_format(F.col("data_registro"),   "yyyyMMdd").cast("int").alias("fk_tempo_registro"),
        # Atributos degenerados (da transação)
        F.col("id_apolice"),
        F.col("numero_apolice"),
        F.col("tipo_sinistro"),
        F.col("status_sinistro"),
        # Métricas (fatos numéricos)
        F.col("valor_prejuizo"),
        F.col("valor_premio"),
        F.when(F.col("valor_prejuizo").isNotNull() & (F.col("valor_premio") > 0),
               F.round(F.col("valor_prejuizo") / F.col("valor_premio"), 4)
              ).alias("indice_sinistralidade"),
        F.lit(1).alias("qtd_sinistros"),
        # Metadados
        F.current_timestamp().alias("_gold_timestamp"),
    )
)

total_fato = df_fato_sinistros.count()

(
    df_fato_sinistros.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("GOLD.FATO_SINISTROS")
)

print(f"✅ GOLD.FATO_SINISTROS: {total_fato} registros")
display(df_fato_sinistros.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 8. Consultas Analíticas de Validação (SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📊 Análise 1: Total de sinistros e prejuízo médio por tipo
# MAGIC SELECT
# MAGIC     tipo_sinistro,
# MAGIC     COUNT(*)                          AS qtd_sinistros,
# MAGIC     SUM(valor_prejuizo)               AS total_prejuizo,
# MAGIC     ROUND(AVG(valor_prejuizo), 2)     AS prejuizo_medio,
# MAGIC     ROUND(AVG(indice_sinistralidade), 4) AS sinistralidade_media
# MAGIC FROM GOLD.FATO_SINISTROS
# MAGIC GROUP BY tipo_sinistro
# MAGIC ORDER BY total_prejuizo DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📊 Análise 2: Sinistros por tipo de cobertura (Star Schema JOIN)
# MAGIC SELECT
# MAGIC     c.tipo_cobertura,
# MAGIC     c.nivel_cobertura,
# MAGIC     COUNT(f.sk_sinistro)          AS qtd_sinistros,
# MAGIC     SUM(f.valor_prejuizo)         AS total_prejuizo,
# MAGIC     SUM(f.valor_premio)           AS total_premios
# MAGIC FROM GOLD.FATO_SINISTROS f
# MAGIC JOIN  GOLD.DIM_COBERTURA  c ON f.fk_cobertura = c.sk_cobertura
# MAGIC GROUP BY c.tipo_cobertura, c.nivel_cobertura
# MAGIC ORDER BY c.nivel_cobertura

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📊 Análise 3: Top 5 clientes com maior prejuízo acumulado
# MAGIC SELECT
# MAGIC     cl.nome_cliente,
# MAGIC     cl.estado,
# MAGIC     COUNT(f.sk_sinistro)      AS qtd_sinistros,
# MAGIC     SUM(f.valor_prejuizo)     AS total_prejuizo
# MAGIC FROM GOLD.FATO_SINISTROS f
# MAGIC JOIN  GOLD.DIM_CLIENTE    cl ON f.fk_cliente = cl.sk_cliente
# MAGIC GROUP BY cl.nome_cliente, cl.estado
# MAGIC ORDER BY total_prejuizo DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📊 Análise 4: Sazonalidade — sinistros por mês e ano
# MAGIC SELECT
# MAGIC     t.ano,
# MAGIC     t.mes,
# MAGIC     t.nome_mes,
# MAGIC     COUNT(f.sk_sinistro)  AS qtd_sinistros,
# MAGIC     SUM(f.valor_prejuizo) AS total_prejuizo
# MAGIC FROM GOLD.FATO_SINISTROS f
# MAGIC JOIN  GOLD.DIM_TEMPO      t ON f.fk_tempo_ocorrencia = t.sk_tempo
# MAGIC GROUP BY t.ano, t.mes, t.nome_mes
# MAGIC ORDER BY t.ano, t.mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 9. Resumo Final — Schema GOLD

# COMMAND ----------

tabelas_gold = ["DIM_CLIENTE", "DIM_VEICULO", "DIM_COBERTURA", "DIM_LOCALIDADE", "DIM_TEMPO", "FATO_SINISTROS"]

print("🏆 RESUMO DO SCHEMA GOLD — Modelagem Dimensional (Ralph Kimball)\n")
print(f"{'Tabela':<20} {'Tipo':<12} {'Registros':>10}")
print("-" * 45)

tipos = {
    "DIM_CLIENTE":    "Dimensão",
    "DIM_VEICULO":    "Dimensão",
    "DIM_COBERTURA":  "Dimensão",
    "DIM_LOCALIDADE": "Dimensão",
    "DIM_TEMPO":      "Dimensão",
    "FATO_SINISTROS": "Fato",
}

for tabela in tabelas_gold:
    try:
        total = spark.table(f"GOLD.{tabela}").count()
        tipo  = tipos.get(tabela, "")
        print(f"  {tabela:<18} {tipo:<12} {total:>10,}")
    except Exception as e:
        print(f"  {tabela:<18} {'ERRO':<12} {str(e)[:30]}")

print("\n🏁 Notebook 04 finalizado — GOLD (Star Schema) pronto para consumo analítico!")
