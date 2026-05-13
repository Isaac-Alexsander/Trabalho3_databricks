# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Notebook 01 — Extração: PostgreSQL (Supabase) → LANDING/DADOS
# MAGIC
# MAGIC **Pipeline:** `Supabase (PostgreSQL)` → `LANDING/DADOS` (CSV)
# MAGIC
# MAGIC | Parâmetro | Valor |
# MAGIC |-----------|-------|
# MAGIC | Origem    | PostgreSQL — Supabase |
# MAGIC | Destino   | Volume `workspace.landing.dados` |
# MAGIC | Formato   | CSV |
# MAGIC | Tabelas   | 11 tabelas — Seguro de Veículos |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 1. Configuração JDBC
# MAGIC ⚠️ **Preencha com suas credenciais do Supabase antes de rodar**

# COMMAND ----------

SUPABASE_HOST     = "db.iykxjhjcimxnyzvzfnit.supabase.co"
SUPABASE_PORT     = "5432"
SUPABASE_DB       = "postgres"
SUPABASE_USER     = "postgres.iykxjhjcimxnyzvzfnit"
SUPABASE_PASSWORD = "SUA_SENHA_AQUI"

JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}?sslmode=require"

print("✅ Parâmetros JDBC configurados.")
print(f"   Host : {SUPABASE_HOST}")
print(f"   User : {SUPABASE_USER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 2. Verificação do Schema LANDING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS LANDING;
# MAGIC SELECT 'Schema LANDING OK' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 3. Tabelas para Extração

# COMMAND ----------

TABELAS = [
    "regiao", "estado", "municipio", "marca", "modelo",
    "cliente", "endereco", "telefone", "carro", "apolice", "sinistro",
]
print(f"📦 Total de tabelas: {len(TABELAS)}")
for t in TABELAS:
    print(f"   → {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 4. Extração: Supabase → CSV no Volume

# COMMAND ----------

from datetime import datetime

resultados = []

for tabela in TABELAS:
    inicio = datetime.now()
    print(f"\n⏳ Extraindo: {tabela} ...")

    try:
        df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", f"public.{tabela}")
            .option("driver", "org.postgresql.Driver")
            .option("user", SUPABASE_USER)
            .option("password", SUPABASE_PASSWORD)
            .option("fetchsize", "10000")
            .load()
        )

        total_linhas = df.count()
        caminho = f"/Volumes/workspace/landing/dados/{tabela}"

        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .csv(caminho)
        )

        duracao = (datetime.now() - inicio).seconds
        resultados.append({"tabela": tabela, "status": "✅ OK", "linhas": total_linhas, "duracao_s": duracao})
        print(f"   ✅ {tabela}: {total_linhas} linhas gravadas ({duracao}s)")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": "❌ ERRO", "linhas": 0, "erro": str(e)})
        print(f"   ❌ ERRO em {tabela}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 5. Relatório

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_rel = StructType([
    StructField("tabela",    StringType(),  True),
    StructField("status",    StringType(),  True),
    StructField("linhas",    IntegerType(), True),
    StructField("duracao_s", IntegerType(), True),
])

dados_rel = [(r["tabela"], r["status"], r.get("linhas", 0), r.get("duracao_s", 0)) for r in resultados]
df_rel = spark.createDataFrame(dados_rel, schema_rel)
display(df_rel)

total_ok = sum(1 for r in resultados if "OK" in r["status"])
print(f"\n✅ Tabelas extraídas: {total_ok}/{len(TABELAS)}")
print(f"   Total de linhas  : {sum(r.get('linhas',0) for r in resultados)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 6. Validação dos CSVs

# COMMAND ----------

print("🔍 Validando CSVs no Volume:\n")
for tabela in TABELAS:
    try:
        df_check = spark.read.option("header", "true").csv(f"/Volumes/workspace/landing/dados/{tabela}")
        print(f"  ✅ {tabela:<15} | {df_check.count()} linhas | {len(df_check.columns)} colunas")
    except Exception as e:
        print(f"  ❌ {tabela:<15} | ERRO: {e}")

print("\n🏁 Notebook 01 finalizado — LANDING populado!")
