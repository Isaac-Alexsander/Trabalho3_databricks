# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Notebook 03 — Data Quality: BRONZE → SILVER
# MAGIC
# MAGIC **Pipeline:** `BRONZE` (Delta bruto) → `SILVER` (Delta tratado)
# MAGIC
# MAGIC ### Regras aplicadas:
# MAGIC | # | Regra | Tabelas |
# MAGIC |---|-------|---------|
# MAGIC | 1 | Remoção de duplicatas por PK | Todas |
# MAGIC | 2 | Remoção de nulos obrigatórios | Todas |
# MAGIC | 3 | Padronização de strings (trim + initcap) | cliente, marca, modelo |
# MAGIC | 4 | Validação de CPF (11 dígitos) | cliente |
# MAGIC | 5 | Validação de datas (inicio < fim) | apolice |
# MAGIC | 6 | Valores monetários não negativos | apolice, sinistro |
# MAGIC | 7 | Padronização de enumerações (lowercase) | apolice, sinistro, carro |
# MAGIC | 8 | Remoção de metadados de ingestão | Todas |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 1. Criação do Schema SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS SILVER;
# MAGIC SELECT 'Schema SILVER OK' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 2. Funções de Data Quality

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime

def remover_duplicatas(df: DataFrame, pk_cols: list) -> tuple:
    total_antes = df.count()
    df_limpo = df.dropDuplicates(pk_cols)
    removidos = total_antes - df_limpo.count()
    if removidos > 0:
        print(f"     ⚠️  Duplicatas removidas: {removidos}")
    return df_limpo, removidos

def remover_nulos_criticos(df: DataFrame, cols_obrigatorias: list) -> tuple:
    total_antes = df.count()
    df_limpo = df.dropna(subset=cols_obrigatorias)
    removidos = total_antes - df_limpo.count()
    if removidos > 0:
        print(f"     ⚠️  Nulos críticos removidos: {removidos}")
    return df_limpo, removidos

def padronizar_strings(df: DataFrame, cols: list) -> DataFrame:
    for col in cols:
        if col in df.columns:
            df = df.withColumn(col, F.initcap(F.trim(F.col(col))))
    return df

def remover_metadados(df: DataFrame) -> DataFrame:
    cols_meta = ["_ingestao_timestamp", "_origem", "_formato_origem"]
    return df.drop(*[c for c in cols_meta if c in df.columns])

def adicionar_metadados_silver(df: DataFrame, nome_tabela: str) -> DataFrame:
    return (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_tabela",    F.lit(nome_tabela))
    )

print("✅ Funções de Data Quality definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 3. Regras por Tabela

# COMMAND ----------

REGRAS = {
    "regiao":    {"pk": ["id_regiao"],    "obrigatorios": ["id_regiao", "nome_regiao"],                              "strings": ["nome_regiao"]},
    "estado":    {"pk": ["id_estado"],    "obrigatorios": ["id_estado", "nome_estado", "uf", "id_regiao"],            "strings": ["nome_estado"]},
    "municipio": {"pk": ["id_municipio"], "obrigatorios": ["id_municipio", "nome_municipio", "id_estado"],            "strings": ["nome_municipio"]},
    "marca":     {"pk": ["id_marca"],     "obrigatorios": ["id_marca", "nome_marca"],                                 "strings": ["nome_marca"]},
    "modelo":    {"pk": ["id_modelo"],    "obrigatorios": ["id_modelo", "nome_modelo", "id_marca", "ano_modelo"],     "strings": ["nome_modelo"]},
    "cliente":   {"pk": ["id_cliente"],   "obrigatorios": ["id_cliente", "nome", "cpf", "data_nasc"],                 "strings": ["nome"]},
    "endereco":  {"pk": ["id_endereco"],  "obrigatorios": ["id_endereco", "id_cliente", "id_municipio", "logradouro"],"strings": ["logradouro", "bairro"]},
    "telefone":  {"pk": ["id_telefone"],  "obrigatorios": ["id_telefone", "id_cliente", "ddd", "numero"],             "strings": []},
    "carro":     {"pk": ["id_carro"],     "obrigatorios": ["id_carro", "id_modelo", "id_cliente", "placa", "chassi"], "strings": ["cor"]},
    "apolice":   {"pk": ["id_apolice"],   "obrigatorios": ["id_apolice", "id_carro", "id_cliente", "numero_apolice", "data_inicio", "data_fim", "valor_premio"], "strings": []},
    "sinistro":  {"pk": ["id_sinistro"],  "obrigatorios": ["id_sinistro", "id_apolice", "data_ocorrencia", "status_sinistro"], "strings": []},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 4. Processamento Bronze → Silver

# COMMAND ----------

resultados = []

for tabela, regras in REGRAS.items():
    inicio = datetime.now()
    print(f"\n⏳ Aplicando DQ em: {tabela} ...")

    try:
        df = spark.table(f"BRONZE.{tabela}")
        total_entrada = df.count()

        df = remover_metadados(df)
        df, _ = remover_duplicatas(df, regras["pk"])
        df, _ = remover_nulos_criticos(df, regras["obrigatorios"])

        if regras.get("strings"):
            df = padronizar_strings(df, regras["strings"])

        # Regras específicas por tabela
        if tabela == "cliente":
            df = df.filter(F.length(F.regexp_replace(F.col("cpf"), "[^0-9]", "")) == 11)
            df = df.withColumn("sexo", F.upper(F.col("sexo")))

        elif tabela == "apolice":
            df = df.filter(F.col("data_inicio") < F.col("data_fim"))
            df = df.filter(F.col("valor_premio") > 0)
            df = df.withColumn("status",         F.lower(F.trim(F.col("status"))))
            df = df.withColumn("tipo_cobertura", F.lower(F.trim(F.col("tipo_cobertura"))))

        elif tabela == "sinistro":
            df = df.filter(F.col("valor_prejuizo").isNull() | (F.col("valor_prejuizo") >= 0))
            df = df.withColumn("status_sinistro", F.lower(F.trim(F.col("status_sinistro"))))

        elif tabela == "carro":
            df = df.withColumn("placa",  F.upper(F.trim(F.col("placa"))))
            df = df.withColumn("chassi", F.upper(F.trim(F.col("chassi"))))

        df = adicionar_metadados_silver(df, tabela)
        total_saida = df.count()

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"SILVER.{tabela}")
        )

        duracao = (datetime.now() - inicio).seconds
        descartados = total_entrada - total_saida
        resultados.append({"tabela": tabela, "status": "✅ OK", "entrada": total_entrada, "saida": total_saida, "descartados": descartados, "duracao_s": duracao})
        print(f"   ✅ SILVER.{tabela}: {total_entrada} → {total_saida} registros (descartados: {descartados}) [{duracao}s]")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": "❌ ERRO", "entrada": 0, "saida": 0, "descartados": 0, "erro": str(e)})
        print(f"   ❌ ERRO em {tabela}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 5. Relatório de Data Quality

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_rel = StructType([
    StructField("tabela",      StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("entrada",     IntegerType(), True),
    StructField("saida",       IntegerType(), True),
    StructField("descartados", IntegerType(), True),
    StructField("duracao_s",   IntegerType(), True),
])

dados_rel = [(r["tabela"], r["status"], r.get("entrada",0), r.get("saida",0), r.get("descartados",0), r.get("duracao_s",0)) for r in resultados]
df_rel = spark.createDataFrame(dados_rel, schema_rel)
display(df_rel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 6. Validação Final — SILVER

# COMMAND ----------

print("🔍 Validando tabelas no SILVER:\n")
for tabela in REGRAS.keys():
    try:
        df_check = spark.table(f"SILVER.{tabela}")
        print(f"  ✅ SILVER.{tabela:<15} | {df_check.count()} registros | {len(df_check.columns)} colunas")
    except Exception as e:
        print(f"  ❌ SILVER.{tabela:<15} | ERRO: {e}")

print(f"\n🏁 Notebook 03 finalizado — SILVER com Data Quality aplicado!")
total_descartados = sum(r.get("descartados", 0) for r in resultados)
print(f"   Registros descartados pelo DQ: {total_descartados}")
