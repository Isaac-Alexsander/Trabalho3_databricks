<div align="center">

# 🏆 Trabalho 3 — Lakehouse com Databricks
### Arquitetura Medalhão • Delta Lake • Ralph Kimball

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)

[![MkDocs](https://img.shields.io/badge/📚_Documentação_Online-MkDocs-blue?style=for-the-badge)](https://Isaac-Alexsander.github.io/Trabalho3_databricks/)

---

**🎓 SATC** — Associação Beneficente da Indústria Carbonífera de Santa Catarina  
**📘 Engenharia de Software** • 5ª Fase • **Engenharia de Dados**  
**👨‍🏫 Professor:** Jorge Luiz Silva • **👨‍💻 Acadêmico:** Isaac Alexsander Pereira Pessoa

</div>

---

## 🎯 Sobre o Projeto

Pipeline de dados **end-to-end** implementado em **Databricks Free Edition (Serverless)**, seguindo a **Arquitetura Medalhão** (Landing → Bronze → Silver → Gold) com orquestração via **Jobs & Pipelines**. O domínio modelado é o de **Seguro de Veículos**, dando continuidade aos Trabalhos 1 e 2.

> **Diferencial:** Toda a infraestrutura é **PaaS na nuvem** — sem Docker, sem WSL, sem drivers manuais. Resolve definitivamente os gargalos de conectividade dos trabalhos anteriores.

---

## 🏛️ Arquitetura do Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Supabase   │     │  🟢 LANDING  │     │  🥉 BRONZE   │     │  🥈 SILVER   │     │  🥇 GOLD     │
│  PostgreSQL  │ ──► │   CSV bruto  │ ──► │  Delta Lake  │ ──► │  Delta + DQ  │ ──► │ Star Schema  │
│  11 tabelas  │     │  (extração)  │     │  (ingestão)  │     │ (qualidade)  │     │   (Kimball)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                          ▲                                                                  │
                          │                                                                  ▼
                    API REST HTTPS                                                  📊 Analytics / BI
                  (Databricks Free Edition)                                         (Power BI, etc.)

                            ◀══════════ ORQUESTRADO POR DATABRICKS JOBS ══════════▶
```

---

## 🛠️ Stack Tecnológica

| Tecnologia | Papel no Projeto |
|:--|:--|
| 🟧 **Databricks Free Edition** | Plataforma PaaS de execução (Serverless) |
| ⚡ **Apache Spark 3.5** | Motor de processamento distribuído |
| 🔺 **Delta Lake** | Armazenamento ACID (Bronze, Silver, Gold) |
| 🟢 **Supabase / PostgreSQL** | Banco de dados de origem |
| 🐍 **Python** | Notebooks de pipeline |
| 🗃️ **SQL** | Consultas analíticas no Star Schema |
| 📚 **MkDocs Material** | Documentação técnica publicada |

---

## 🗃️ Domínio de Dados — Seguro de Veículos

### Modelo Entidade-Relacionamento

```
REGIAO ──< ESTADO ──< MUNICIPIO ──< ENDERECO >── CLIENTE
                                                    │
                                       ┌────────────┼────────────┐
                                       │            │            │
                                     CARRO      TELEFONE      APOLICE
                                       │                         │
                                    MODELO                    SINISTRO
                                       │
                                     MARCA
```

### Tabelas de Origem

| Tabela | Registros | Domínio |
|:--|--:|:--|
| `regiao` | 5 | Regiões do Brasil |
| `estado` | 6 | UFs (SC, RS, PR, SP, MG, RJ) |
| `municipio` | 11 | Cidades dos clientes |
| `marca` | 6 | Marcas de veículos |
| `modelo` | 14 | Modelos de veículos |
| `cliente` | 10 | Segurados |
| `endereco` | 10 | Endereços dos clientes |
| `telefone` | 13 | Contatos telefônicos |
| `carro` | 10 | Veículos segurados |
| `apolice` | 10 | Apólices ativas |
| `sinistro` | 10 | Ocorrências registradas |

---

## 📐 Camadas do Lakehouse

### 🟢 LANDING — Dados Brutos
- **Formato:** CSV com header
- **Origem:** Supabase via **API REST (HTTPS)**
- **Localização:** `/Volumes/workspace/landing/dados/<tabela>/`
- **Características:** Snapshot exato da fonte, sem transformação

> 💡 **Por que API REST e não JDBC?** O Databricks Free Edition usa Serverless, que bloqueia conexões TCP externas. A API REST do Supabase usa HTTPS — mesma extração programática, protocolo compatível.

### 🥉 BRONZE — Ingestão em Delta Lake
- **Formato:** Delta Lake
- **Schema:** `BRONZE`
- **Metadados adicionados:** `_ingestao_timestamp`, `_origem`, `_formato_origem`
- **Benefícios:** Transações ACID, Time Travel, Schema Enforcement

### 🥈 SILVER — Data Quality
- **Formato:** Delta Lake
- **Schema:** `SILVER`
- **8 categorias de regras aplicadas:**
  - ✓ Deduplicação por chave primária
  - ✓ Remoção de nulos em campos obrigatórios
  - ✓ Padronização de strings (trim + initcap)
  - ✓ Validação de CPF (11 dígitos numéricos)
  - ✓ Validação de datas (`data_inicio < data_fim`)
  - ✓ Valores monetários não negativos
  - ✓ Padronização de enumerações (lowercase)
  - ✓ Auditoria com timestamps Silver

### 🥇 GOLD — Modelagem Dimensional (Ralph Kimball)

**Star Schema** com 5 dimensões orbitando 1 tabela fato:

| Tabela | Tipo | Grão |
|:--|:--|:--|
| ⭐ `FATO_SINISTROS` | Fato | 1 linha por sinistro |
| 👤 `DIM_CLIENTE` | Dimensão | 1 linha por cliente (endereço desnormalizado) |
| 🚗 `DIM_VEICULO` | Dimensão | 1 linha por veículo (marca + modelo) |
| 🛡️ `DIM_COBERTURA` | Dimensão | 1 linha por tipo de cobertura |
| 📍 `DIM_LOCALIDADE` | Dimensão | 1 linha por município |
| 📅 `DIM_TEMPO` | Dimensão | Calendário gerado a partir dos dados |

**Métricas da Fato:** `valor_prejuizo`, `valor_premio`, `indice_sinistralidade`, `qtd_sinistros`

---

## ⚙️ Automação — Jobs & Pipelines

Pipeline orquestrado em uma única Job do Databricks com dependências sequenciais:

```
┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐
│ 01_landing_extracao│──►│  02_bronze_ingestao│──►│03_silver_data_qual.│──►│04_gold_modelagem_d.│
│   Supabase → CSV   │   │    CSV → Delta     │   │  Delta + DQ rules  │   │   Star Schema      │
└────────────────────┘   └────────────────────┘   └────────────────────┘   └────────────────────┘
```

> 📌 Compute: **Serverless** • Modo: **Sequencial com dependências (depends_on)**

---

## 🚀 Como Reproduzir

### Pré-requisitos

- Conta gratuita no [**Databricks Free Edition**](https://community.cloud.databricks.com/)
- Conta gratuita no [**Supabase**](https://supabase.com/)
- [GitHub](https://github.com/) (para versionamento e MkDocs)

> ✅ **Zero instalação local.** Todo o processamento é em nuvem.

### Passo 1 — Banco de Dados (Supabase)

1. Crie um projeto novo no Supabase
2. Vá em **SQL Editor** → cole e execute [`data/01_setup_supabase.sql`](data/01_setup_supabase.sql)
3. Desabilite o RLS para permitir leitura via API REST:
   ```sql
   ALTER TABLE regiao    DISABLE ROW LEVEL SECURITY;
   ALTER TABLE estado    DISABLE ROW LEVEL SECURITY;
   -- (repetir para as 11 tabelas)
   ```
4. Em **Project Settings → API Keys**, copie a `anon public` key

### Passo 2 — Databricks

1. No Databricks, crie a estrutura de armazenamento:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.landing;
   CREATE VOLUME  IF NOT EXISTS workspace.landing.dados;
   CREATE SCHEMA IF NOT EXISTS BRONZE;
   CREATE SCHEMA IF NOT EXISTS SILVER;
   CREATE SCHEMA IF NOT EXISTS GOLD;
   ```

2. Importe os 4 notebooks da pasta `notebooks/` no Workspace
3. No notebook `01_landing_extracao`, preencha:
   ```python
   SUPABASE_URL = "https://SEU_PROJETO.supabase.co"
   SUPABASE_KEY = "SUA_ANON_KEY_AQUI"
   ```

### Passo 3 — Job de Automação

1. Acesse **Jobs & Pipelines** → **Create Job**
2. Nome: `Pipeline_SeguroDB_Medalhao`
3. Crie 4 tasks **em sequência** (cada uma depende da anterior):

| # | Task | Notebook | Compute |
|:-:|:--|:--|:--|
| 1 | `01_landing_extracao` | `01_landing_extracao` | Serverless |
| 2 | `02_bronze_ingestao` | `02_bronze_ingestao` | Serverless |
| 3 | `03_silver_data_quality` | `03_silver_data_quality` | Serverless |
| 4 | `04_gold_modelagem_dimensional` | `04_gold_modelagem_dimensional` | Serverless |

4. Clique em **Run Now** ▶️

---

## 📁 Estrutura do Repositório

```
trabalho3_databricks/
│
├── 📂 data/
│   └── 01_setup_supabase.sql            # DDL + carga inicial
│
├── 📂 notebooks/
│   ├── 01_landing_extracao.py           # Supabase → LANDING (CSV)
│   ├── 02_bronze_ingestao.py            # LANDING → BRONZE (Delta)
│   ├── 03_silver_data_quality.py        # BRONZE → SILVER (Delta + DQ)
│   └── 04_gold_modelagem_dimensional.py # SILVER → GOLD (Star Schema)
│
├── 📂 docs/                              # MkDocs (Material Theme)
│   ├── index.md                         # Visão geral
│   ├── arquitetura.md                   # Arquitetura Medalhão
│   ├── databricks.md                    # Setup do Databricks
│   ├── pipeline.md                      # Detalhamento por camada
│   ├── gold_kimball.md                  # Modelagem Ralph Kimball
│   └── stylesheets/extra.css
│
├── 📄 README.md                          # Este arquivo
├── 📄 mkdocs.yml                         # Config do MkDocs
├── 📄 pyproject.toml                     # Dependências (uv)
└── 📄 .gitignore
```

---

## 🌐 Links do Projeto

| Recurso | Link |
|:--|:--|
| 📚 **Documentação MkDocs** | [Isaac-Alexsander.github.io/Trabalho3_databricks](https://Isaac-Alexsander.github.io/Trabalho3_databricks/) |
| 💻 **Repositório GitHub** | [github.com/Isaac-Alexsander/Trabalho3_databricks](https://github.com/Isaac-Alexsander/Trabalho3_databricks) |

---

## 📚 Referências

- [📘 Databricks — Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [📘 Delta Lake — Documentação Oficial](https://docs.delta.io/latest/index.html)
- [📘 Supabase — Documentação](https://supabase.com/docs)
- [📘 Ralph Kimball — The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)
- [🎬 Canal DataWay BR](https://www.youtube.com/@DataWayBR)
- [💾 Repositório base do professor](https://github.com/jlsilva01/spark-delta-minio-sqlserver)

---

<div align="center">

📜 **Projeto acadêmico** • SATC • Engenharia de Software • 5ª Fase • 2025

</div>
