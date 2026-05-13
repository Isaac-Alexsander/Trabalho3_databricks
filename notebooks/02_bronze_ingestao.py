# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Notebook 02 — Ingestão: LANDING/DADOS → BRONZE (Delta Lake)
# MAGIC
# MAGIC **Pipeline:** `LANDING/DADOS` (CSV) → `BRONZE` (Delta Lake)
# MAGIC
# MAGIC | Parâmetro | Valor |
# MAGIC |-----------|-------|
# MAGIC | Origem    | Volume `workspace.landing.dados` (CSV) |
# MAGIC | Destino   | Schema `BRONZE` (Delta Lake) |
# MAGIC | Modo      | overwrite |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 1. Criação do Schema BRONZE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS BRONZE;
# MAGIC SELECT 'Schema BRONZE OK' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 2. Lista de Tabelas

# COMMAND ----------

TABELAS = [
    "regiao", "estado", "municipio", "marca", "modelo",
    "cliente", "endereco", "telefone", "carro", "apolice", "sinistro",
]
print(f"📦 Tabelas para ingestão no BRONZE: {len(TABELAS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 3. Ingestão CSV → Delta Lake

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

resultados = []

for tabela in TABELAS:
    inicio = datetime.now()
    print(f"\n⏳ Processando: {tabela} ...")

    try:
        caminho_csv = f"/Volumes/workspace/landing/dados/{tabela}"

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .csv(caminho_csv)
        )

        # Adiciona metadados de ingestão
        df = (
            df
            .withColumn("_ingestao_timestamp", F.current_timestamp())
            .withColumn("_origem",              F.lit(f"landing/dados/{tabela}"))
            .withColumn("_formato_origem",      F.lit("CSV"))
        )

        total_linhas = df.count()

        # Grava em Delta Lake no schema BRONZE
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"BRONZE.{tabela}")
        )

        duracao = (datetime.now() - inicio).seconds
        resultados.append({"tabela": tabela, "status": "✅ OK", "linhas": total_linhas, "colunas": len(df.columns), "duracao_s": duracao})
        print(f"   ✅ BRONZE.{tabela}: {total_linhas} linhas, {len(df.columns)} cols ({duracao}s)")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": "❌ ERRO", "linhas": 0, "colunas": 0, "erro": str(e)})
        print(f"   ❌ ERRO em {tabela}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 4. Relatório

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_rel = StructType([
    StructField("tabela",    StringType(),  True),
    StructField("status",    StringType(),  True),
    StructField("linhas",    IntegerType(), True),
    StructField("colunas",   IntegerType(), True),
    StructField("duracao_s", IntegerType(), True),
])

dados_rel = [(r["tabela"], r["status"], r.get("linhas",0), r.get("colunas",0), r.get("duracao_s",0)) for r in resultados]
df_rel = spark.createDataFrame(dados_rel, schema_rel)
display(df_rel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 5. Validação das Tabelas Delta no BRONZE

# COMMAND ----------

print("🔍 Validando tabelas Delta no BRONZE:\n")
for tabela in TABELAS:
    try:
        df_check = spark.table(f"BRONZE.{tabela}")
        print(f"  ✅ BRONZE.{tabela:<15} | {df_check.count()} linhas | {len(df_check.columns)} colunas")
    except Exception as e:
        print(f"  ❌ BRONZE.{tabela:<15} | ERRO: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🕒 6. Delta Lake — Time Travel

# COMMAND ----------

print("📜 Histórico de versões — BRONZE.sinistro:")
display(spark.sql("DESCRIBE HISTORY BRONZE.sinistro"))

# COMMAND ----------

print("\n🏁 Notebook 02 finalizado — BRONZE populado com Delta Lake!")
print(f"   Tabelas OK: {sum(1 for r in resultados if 'OK' in r['status'])}/{len(TABELAS)}")
