# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Notebook 03 — Data Quality: BRONZE → SILVER
# MAGIC
# MAGIC **Pipeline:** `BRONZE` (Delta bruto) → `SILVER` (Delta tratado e confiável)
# MAGIC
# MAGIC **Objetivo:** Aplicar regras de **Data Quality** nos dados da camada Bronze,
# MAGIC garantindo consistência, completude e confiabilidade antes da modelagem dimensional.
# MAGIC
# MAGIC ### Regras de Data Quality aplicadas:
# MAGIC
# MAGIC | # | Regra | Tabelas |
# MAGIC |---|-------|---------|
# MAGIC | 1 | Remoção de duplicatas (dedup por PK) | Todas |
# MAGIC | 2 | Padronização de strings (trim, upper/lower) | cliente, marca, modelo |
# MAGIC | 3 | Validação de nulos em campos obrigatórios | Todas |
# MAGIC | 4 | Validação de CPF (11 dígitos numéricos) | cliente |
# MAGIC | 5 | Validação de datas (data_inicio < data_fim) | apolice |
# MAGIC | 6 | Valores monetários não negativos | apolice, sinistro |
# MAGIC | 7 | Remoção de colunas de metadados de ingestão | Todas |
# MAGIC | 8 | Cast de tipos de dados | Todas |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗃️ 1. Criação do Schema SILVER

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS SILVER")
print("✅ Schema SILVER criado/verificado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 2. Funções de Data Quality

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime

def remover_duplicatas(df: DataFrame, pk_cols: list) -> tuple:
    """Remove duplicatas baseando-se nas colunas de chave primária."""
    total_antes = df.count()
    df_limpo = df.dropDuplicates(pk_cols)
    total_depois = df_limpo.count()
    removidos = total_antes - total_depois
    if removidos > 0:
        print(f"     ⚠️  Duplicatas removidas: {removidos}")
    return df_limpo, removidos

def remover_nulos_criticos(df: DataFrame, cols_obrigatorias: list) -> tuple:
    """Remove linhas onde campos obrigatórios são nulos."""
    total_antes = df.count()
    df_limpo = df.dropna(subset=cols_obrigatorias)
    total_depois = df_limpo.count()
    removidos = total_antes - total_depois
    if removidos > 0:
        print(f"     ⚠️  Nulos em campos obrigatórios removidos: {removidos}")
    return df_limpo, removidos

def padronizar_strings(df: DataFrame, cols: list) -> DataFrame:
    """Aplica trim e capitalização padronizada em colunas de texto."""
    for col in cols:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
            df = df.withColumn(col, F.initcap(F.col(col)))
    return df

def remover_metadados_ingestao(df: DataFrame) -> DataFrame:
    """Remove colunas técnicas adicionadas na camada Bronze."""
    cols_meta = ["_ingestao_timestamp", "_origem", "_formato_origem"]
    cols_remover = [c for c in cols_meta if c in df.columns]
    if cols_remover:
        df = df.drop(*cols_remover)
    return df

def adicionar_metadados_silver(df: DataFrame, nome_tabela: str) -> DataFrame:
    """Adiciona colunas de auditoria da camada Silver."""
    return (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_tabela",    F.lit(nome_tabela))
    )

print("✅ Funções de Data Quality definidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 3. Regras por Tabela e Processamento

# COMMAND ----------

# Configuração das regras por tabela
REGRAS_TABELAS = {
    "regiao": {
        "pk": ["id_regiao"],
        "obrigatorios": ["id_regiao", "nome_regiao"],
        "strings_padronizar": ["nome_regiao"],
    },
    "estado": {
        "pk": ["id_estado"],
        "obrigatorios": ["id_estado", "nome_estado", "uf", "id_regiao"],
        "strings_padronizar": ["nome_estado"],
    },
    "municipio": {
        "pk": ["id_municipio"],
        "obrigatorios": ["id_municipio", "nome_municipio", "id_estado"],
        "strings_padronizar": ["nome_municipio"],
    },
    "marca": {
        "pk": ["id_marca"],
        "obrigatorios": ["id_marca", "nome_marca"],
        "strings_padronizar": ["nome_marca"],
    },
    "modelo": {
        "pk": ["id_modelo"],
        "obrigatorios": ["id_modelo", "nome_modelo", "id_marca", "ano_modelo"],
        "strings_padronizar": ["nome_modelo"],
    },
    "cliente": {
        "pk": ["id_cliente"],
        "obrigatorios": ["id_cliente", "nome", "cpf", "data_nasc"],
        "strings_padronizar": ["nome"],
    },
    "endereco": {
        "pk": ["id_endereco"],
        "obrigatorios": ["id_endereco", "id_cliente", "id_municipio", "logradouro"],
        "strings_padronizar": ["logradouro", "bairro"],
    },
    "telefone": {
        "pk": ["id_telefone"],
        "obrigatorios": ["id_telefone", "id_cliente", "ddd", "numero"],
        "strings_padronizar": [],
    },
    "carro": {
        "pk": ["id_carro"],
        "obrigatorios": ["id_carro", "id_modelo", "id_cliente", "placa", "chassi"],
        "strings_padronizar": ["cor"],
    },
    "apolice": {
        "pk": ["id_apolice"],
        "obrigatorios": ["id_apolice", "id_carro", "id_cliente", "numero_apolice", "data_inicio", "data_fim", "valor_premio"],
        "strings_padronizar": [],
    },
    "sinistro": {
        "pk": ["id_sinistro"],
        "obrigatorios": ["id_sinistro", "id_apolice", "data_ocorrencia", "status_sinistro"],
        "strings_padronizar": [],
    },
}

# COMMAND ----------

resultados = []

for tabela, regras in REGRAS_TABELAS.items():
    inicio = datetime.now()
    print(f"\n⏳ Aplicando DQ em: {tabela} ...")

    try:
        df = spark.table(f"BRONZE.{tabela}")
        total_entrada = df.count()

        # Etapa 1 — Remove metadados de ingestão da Bronze
        df = remover_metadados_ingestao(df)

        # Etapa 2 — Remove duplicatas por PK
        df, dup_removidas = remover_duplicatas(df, regras["pk"])

        # Etapa 3 — Remove nulos em campos obrigatórios
        df, nulos_removidos = remover_nulos_criticos(df, regras["obrigatorios"])

        # Etapa 4 — Padroniza strings
        if regras.get("strings_padronizar"):
            df = padronizar_strings(df, regras["strings_padronizar"])

        # Etapa 5 — Regras específicas por tabela
        if tabela == "cliente":
            # Valida CPF: deve ter exatamente 11 dígitos numéricos
            antes = df.count()
            df = df.filter(F.length(F.regexp_replace(F.col("cpf"), "[^0-9]", "")) == 11)
            inv_cpf = antes - df.count()
            if inv_cpf > 0:
                print(f"     ⚠️  CPFs inválidos removidos: {inv_cpf}")

            # Padroniza sexo para maiúsculo
            df = df.withColumn("sexo", F.upper(F.col("sexo")))

        elif tabela == "apolice":
            # Valida que data_inicio < data_fim
            df = df.filter(F.col("data_inicio") < F.col("data_fim"))
            # Garante valor_premio positivo
            df = df.filter(F.col("valor_premio") > 0)
            # Padroniza status e tipo_cobertura para minúsculo
            df = df.withColumn("status", F.lower(F.trim(F.col("status"))))
            df = df.withColumn("tipo_cobertura", F.lower(F.trim(F.col("tipo_cobertura"))))

        elif tabela == "sinistro":
            # Garante valor_prejuizo não negativo (pode ser nulo)
            df = df.filter(
                F.col("valor_prejuizo").isNull() | (F.col("valor_prejuizo") >= 0)
            )
            # Padroniza status
            df = df.withColumn("status_sinistro", F.lower(F.trim(F.col("status_sinistro"))))

        elif tabela == "carro":
            # Placa em maiúsculo
            df = df.withColumn("placa", F.upper(F.trim(F.col("placa"))))
            # Chassi em maiúsculo
            df = df.withColumn("chassi", F.upper(F.trim(F.col("chassi"))))

        # Etapa 6 — Adiciona metadados Silver
        df = adicionar_metadados_silver(df, tabela)

        total_saida = df.count()
        registros_descartados = total_entrada - total_saida

        # --- Grava em Delta Lake no schema SILVER ---
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"SILVER.{tabela}")
        )

        fim = datetime.now()
        duracao = (fim - inicio).seconds

        resultados.append({
            "tabela":               tabela,
            "status":               "✅ OK",
            "registros_entrada":    total_entrada,
            "registros_saida":      total_saida,
            "registros_descartados": registros_descartados,
            "duracao_s":            duracao,
        })
        print(f"   ✅ SILVER.{tabela}: {total_entrada} → {total_saida} registros "
              f"(descartados: {registros_descartados}) [{duracao}s]")

    except Exception as e:
        resultados.append({"tabela": tabela, "status": "❌ ERRO", "registros_entrada": 0,
                           "registros_saida": 0, "registros_descartados": 0, "erro": str(e)})
        print(f"   ❌ ERRO em {tabela}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 4. Relatório de Data Quality

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_rel = StructType([
    StructField("tabela",               StringType(),  True),
    StructField("status",               StringType(),  True),
    StructField("registros_entrada",    IntegerType(), True),
    StructField("registros_saida",      IntegerType(), True),
    StructField("registros_descartados",IntegerType(), True),
    StructField("duracao_s",            IntegerType(), True),
])

dados_rel = [
    (r["tabela"], r["status"], r.get("registros_entrada", 0),
     r.get("registros_saida", 0), r.get("registros_descartados", 0), r.get("duracao_s", 0))
    for r in resultados
]

df_rel = spark.createDataFrame(dados_rel, schema_rel)
display(df_rel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 5. Validação Final — Schema SILVER

# COMMAND ----------

print("🔍 Validação das tabelas no schema SILVER:\n")

for tabela in REGRAS_TABELAS.keys():
    try:
        df_check = spark.table(f"SILVER.{tabela}")
        print(f"  ✅ SILVER.{tabela:<15} | {df_check.count()} registros | {len(df_check.columns)} colunas")
    except Exception as e:
        print(f"  ❌ SILVER.{tabela:<15} | ERRO: {e}")

# COMMAND ----------

# Amostra dos dados tratados da tabela mais relevante
print("\n📋 Amostra SILVER.apolice (dados tratados):")
display(spark.table("SILVER.apolice").select(
    "id_apolice", "numero_apolice", "tipo_cobertura", "status",
    "valor_premio", "data_inicio", "data_fim", "_silver_timestamp"
).limit(10))

# COMMAND ----------

print("\n🏁 Notebook 03 finalizado — SILVER com Data Quality aplicado!")
total_ok = sum(1 for r in resultados if "OK" in r["status"])
total_descartados = sum(r.get("registros_descartados", 0) for r in resultados)
print(f"   Tabelas processadas : {total_ok}/{len(REGRAS_TABELAS)}")
print(f"   Registros descartados (DQ): {total_descartados}")
