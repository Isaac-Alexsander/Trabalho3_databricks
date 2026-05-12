# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Notebook 01 — Extração: PostgreSQL (Supabase) → LANDING/DADOS
# MAGIC
# MAGIC **Pipeline:** `Supabase (PostgreSQL)` → `LANDING/DADOS` (CSV)
# MAGIC
# MAGIC **Objetivo:** Extrair todas as tabelas do banco SeguroDB hospedado no Supabase
# MAGIC e gravar como arquivos CSV no schema `LANDING/DADOS` do Databricks.
# MAGIC
# MAGIC | Parâmetro | Valor |
# MAGIC |-----------|-------|
# MAGIC | Origem    | PostgreSQL — Supabase |
# MAGIC | Destino   | Schema `LANDING` / `DADOS` |
# MAGIC | Formato   | CSV (dados relacionais) |
# MAGIC | Tabelas   | 11 tabelas do domínio Seguro de Veículos |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 1. Configuração da Conexão JDBC

# COMMAND ----------

# ------------------------------------------------------------------
# Parâmetros de conexão com o Supabase (PostgreSQL)
# Substitua os valores abaixo pelos seus dados do Supabase:
#   Project Settings > Database > Connection string (JDBC)
# ------------------------------------------------------------------

SUPABASE_HOST     = dbutils.secrets.get(scope="segurodb", key="host")
SUPABASE_PORT     = "5432"
SUPABASE_DB       = "postgres"
SUPABASE_USER     = dbutils.secrets.get(scope="segurodb", key="user")
SUPABASE_PASSWORD = dbutils.secrets.get(scope="segurodb", key="password")

JDBC_URL = (
    f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    f"?sslmode=require"
)

JDBC_PROPS = {
    "driver":   "org.postgresql.Driver",
    "user":     SUPABASE_USER,
    "password": SUPABASE_PASSWORD,
}

print("✅ Parâmetros JDBC configurados.")
print(f"   Host : {SUPABASE_HOST}")
print(f"   DB   : {SUPABASE_DB}")
print(f"   URL  : {JDBC_URL.split('?')[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 2. Criação do Schema LANDING no Databricks

# COMMAND ----------

# Cria o catalog/schema de destino caso não exista
spark.sql("CREATE SCHEMA IF NOT EXISTS LANDING")
spark.sql("CREATE SCHEMA IF NOT EXISTS LANDING.DADOS")

print("✅ Schema LANDING.DADOS criado/verificado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 3. Lista de Tabelas para Extração

# COMMAND ----------

# Lista com todas as tabelas do SeguroDB no Supabase
TABELAS = [
    "regiao",
    "estado",
    "municipio",
    "marca",
    "modelo",
    "cliente",
    "endereco",
    "telefone",
    "carro",
    "apolice",
    "sinistro",
]

print(f"📦 Total de tabelas a extrair: {len(TABELAS)}")
for t in TABELAS:
    print(f"   → {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 4. Extração e Carga no LANDING/DADOS

# COMMAND ----------

from datetime import datetime

resultados = []

for tabela in TABELAS:
    inicio = datetime.now()
    print(f"\n⏳ Extraindo tabela: {tabela} ...")

    try:
        # --- Leitura via JDBC do Supabase ---
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

        # --- Grava como CSV no schema LANDING.DADOS ---
        # Usando Unity Catalog: landing.dados.<tabela>
        # O arquivo CSV fica em volumes gerenciados do Databricks
        caminho_destino = f"/Volumes/main/landing/dados/{tabela}"

        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .option("delimiter", ",")
            .option("encoding", "UTF-8")
            .csv(caminho_destino)
        )

        fim = datetime.now()
        duracao = (fim - inicio).seconds

        resultados.append({
            "tabela": tabela,
            "status": "✅ OK",
            "linhas": total_linhas,
            "duracao_s": duracao,
        })
        print(f"   ✅ {tabela}: {total_linhas} linhas → {caminho_destino} ({duracao}s)")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": "❌ ERRO", "linhas": 0, "erro": str(e)})
        print(f"   ❌ ERRO em {tabela}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 5. Relatório de Extração

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_relatorio = StructType([
    StructField("tabela",     StringType(),  True),
    StructField("status",     StringType(),  True),
    StructField("linhas",     IntegerType(), True),
    StructField("duracao_s",  IntegerType(), True),
])

dados_relatorio = [
    (r["tabela"], r["status"], r.get("linhas", 0), r.get("duracao_s", 0))
    for r in resultados
]

df_relatorio = spark.createDataFrame(dados_relatorio, schema_relatorio)
display(df_relatorio)

total_ok    = sum(1 for r in resultados if "OK" in r["status"])
total_erro  = len(resultados) - total_ok
total_linhas = sum(r.get("linhas", 0) for r in resultados)

print(f"\n{'='*50}")
print(f"  RESUMO DA EXTRAÇÃO — LANDING")
print(f"{'='*50}")
print(f"  Tabelas extraídas : {total_ok}/{len(TABELAS)}")
print(f"  Tabelas com erro  : {total_erro}")
print(f"  Total de linhas   : {total_linhas}")
print(f"{'='*50}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 6. Validação — Leitura dos CSVs no LANDING

# COMMAND ----------

print("🔍 Validando arquivos CSV gravados no LANDING/DADOS:\n")

for tabela in TABELAS:
    caminho = f"/Volumes/main/landing/dados/{tabela}"
    try:
        df_check = spark.read.option("header", "true").csv(caminho)
        print(f"  ✅ {tabela:<15} | {df_check.count()} linhas | {len(df_check.columns)} colunas")
    except Exception as e:
        print(f"  ❌ {tabela:<15} | ERRO: {e}")

print("\n🏁 Notebook 01 finalizado — LANDING/DADOS populado com sucesso!")
