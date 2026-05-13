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
SUPABASE_PORT     = "6543"
SUPABASE_DB       = "postgres"
SUPABASE_USER     = "postgres.iykxjhjcimxnyzvzfnit"
SUPABASE_PASSWORD = "22300509fF@-1"

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
from pyspark.sql import functions as F

# Credenciais da API REST do Supabase
SUPABASE_URL = "https://iykxjhjcimxnyzvzfnit.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml5a3hqaGpjaW14bnl6dnpmbml0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg2MDU4OTcsImV4cCI6MjA5NDE4MTg5N30.VHypg3WxI1vxeeD0TSv9SzPtTc95ahdgnIr5aeqHzhk"  # veja instruções abaixo

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

TABELAS = [
    "regiao", "estado", "municipio", "marca", "modelo",
    "cliente", "endereco", "telefone", "carro", "apolice", "sinistro",
]

resultados = []

for tabela in TABELAS:
    inicio = datetime.now()
    print(f"\n⏳ Extraindo: {tabela} ...")

    try:
        # Busca todos os registros via REST API
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{tabela}",
            headers=HEADERS,
            params={"select": "*", "limit": "10000"}
        )
        response.raise_for_status()
        dados = response.json()

        if not dados:
            print(f"   ⚠️ {tabela}: sem dados")
            continue

        # Converte para Spark DataFrame
        df = spark.createDataFrame(dados)
        total_linhas = df.count()

        # Grava como CSV no Volume
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
