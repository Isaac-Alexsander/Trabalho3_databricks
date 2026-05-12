# 🏆 Trabalho 3 — Lakehouse com Databricks & Arquitetura Medalhão

<div align="center">

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

**Instituição:** SATC — Associação Beneficente da Indústria Carbonífera de Santa Catarina  
**Curso:** Engenharia de Software | **Fase:** 5ª  
**Disciplina:** Engenharia de Dados  
**Professor:** Jorge Luiz Silva  
**Acadêmico:** Isaac Alexsander Pereira Pessoa

</div>

---

## 📋 Descrição do Projeto

Este projeto implementa um **pipeline de dados completo** na plataforma **Databricks Free Edition**, seguindo a **Arquitetura Medalhão** (Medallion Architecture). O domínio de dados é o **Seguro de Veículos**, dando continuidade ao banco `SeguroDB` utilizado nos Trabalhos 1 e 2.

O pipeline percorre as quatro camadas da arquitetura medalhão de forma sequencial e automatizada:

```
PostgreSQL (Supabase)
        │
        ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│   LANDING     │ ───► │    BRONZE     │ ───► │    SILVER     │ ───► │     GOLD      │
│  CSV bruto    │      │  Delta Lake   │      │  Delta + DQ   │      │  Star Schema  │
│  (extração)   │      │  (ingestão)   │      │  (qualidade)  │      │  (Kimball)    │
└───────────────┘      └───────────────┘      └───────────────┘      └───────────────┘
        ▲                                                                      │
        │                                                                      ▼
  Supabase DB                                                        Dashboards / BI
  (11 tabelas)
```

---

## 🛠️ Stack Tecnológica

| Tecnologia | Versão | Papel no Projeto |
|---|---|---|
| **Databricks Free Edition** | Community | Plataforma PaaS de execução |
| **Apache Spark** | 3.5+ | Motor de processamento distribuído |
| **Delta Lake** | 3.x | Formato de armazenamento ACID |
| **PostgreSQL** (Supabase) | 15 | Banco de dados de origem |
| **Python** | 3.10+ | Linguagem dos notebooks |
| **SQL** | ANSI | Consultas analíticas na camada Gold |

---

## 🗃️ Domínio de Dados — SeguroDB (Seguro de Veículos)

O banco de dados possui **11 tabelas** distribuídas em 3 grupos:

### Modelo Entidade-Relacionamento

```
REGIAO (1) ──< ESTADO (1) ──< MUNICIPIO (1) ──< ENDERECO >── (1) CLIENTE
                                                                      │
                                                             ┌────────┴────────┐
                                                             │                 │
                                                           CARRO           TELEFONE
                                                             │
                                                         MODELO >── (1) MARCA
                                                             │
                                                          APOLICE
                                                             │
                                                          SINISTRO
```

### Tabelas e volumes

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `regiao` | 5 | Regiões do Brasil |
| `estado` | 6 | Estados (SC, RS, PR, SP, MG, RJ) |
| `municipio` | 11 | Cidades dos clientes |
| `marca` | 6 | Marcas de veículos |
| `modelo` | 14 | Modelos de veículos |
| `cliente` | 10 | Segurados |
| `endereco` | 10 | Endereços dos clientes |
| `telefone` | 12 | Telefones de contato |
| `carro` | 10 | Veículos segurados |
| `apolice` | 10 | Apólices de seguro |
| `sinistro` | 10 | Ocorrências/sinistros |

---

## 🚀 Como Reproduzir o Projeto

### Pré-requisitos

- Conta gratuita no **[Databricks Community Edition](https://community.cloud.databricks.com/)**
- Conta gratuita no **[Supabase](https://supabase.com/)** (PostgreSQL gerenciado)
- Navegador web atualizado

> ⚠️ Nenhuma instalação local é necessária. Todo o processamento ocorre na nuvem.

---

### Passo 1 — Configurar o Banco no Supabase

1. Acesse [supabase.com](https://supabase.com) e crie uma conta gratuita
2. Crie um novo projeto (ex: `segurodb`)
3. Aguarde a provisão do banco (≈ 1 minuto)
4. Acesse **SQL Editor** no painel lateral
5. Cole e execute o conteúdo do arquivo [`data/01_setup_supabase.sql`](data/01_setup_supabase.sql)
6. Verifique que as 11 tabelas foram criadas em **Table Editor**

**Coletando as credenciais JDBC:**

```
Painel Supabase → Project Settings → Database → Connection string → JDBC
```

Você precisará de:
- **Host:** `db.xxxxxxxxxxxxxxxx.supabase.co`
- **Porta:** `5432`
- **Database:** `postgres`
- **User:** `postgres`
- **Password:** *(sua senha do projeto)*

---

### Passo 2 — Configurar o Databricks

#### 2.1 Criar cluster

1. Acesse [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Vá em **Compute → Create Compute**
3. Configure:
   - **Cluster name:** `segurodb-pipeline`
   - **Databricks Runtime:** `14.3 LTS` (ou superior)
   - **Node type:** `Community Optimized`
4. Clique em **Create Compute**

#### 2.2 Instalar o driver PostgreSQL (JDBC)

No cluster criado, vá em **Libraries → Install New**:

- **Library Source:** Maven
- **Coordinates:** `org.postgresql:postgresql:42.7.3`
- Clique em **Install**

#### 2.3 Criar Secrets (credenciais seguras)

No terminal do Databricks ou via Databricks CLI:

```bash
# Criar o secret scope
databricks secrets create-scope segurodb

# Adicionar as credenciais
databricks secrets put-secret segurodb host
databricks secrets put-secret segurodb user
databricks secrets put-secret segurodb password
```

> 💡 **Alternativa rápida para teste:** Substitua `dbutils.secrets.get(...)` por strings diretas no notebook 01, mas **nunca faça commit** com credenciais em texto puro.

#### 2.4 Criar os Volumes (Unity Catalog)

No Databricks SQL ou em um notebook:

```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.landing;
CREATE VOLUME  IF NOT EXISTS main.landing.dados;
```

---

### Passo 3 — Importar os Notebooks

1. No Databricks, clique em **Workspace → Import**
2. Selecione **File** e importe cada `.py` da pasta `notebooks/` na ordem:

| Ordem | Arquivo | Camada |
|-------|---------|--------|
| 1 | `01_landing_extracao.py` | Landing |
| 2 | `02_bronze_ingestao.py` | Bronze |
| 3 | `03_silver_data_quality.py` | Silver |
| 4 | `04_gold_modelagem_dimensional.py` | Gold |

---

### Passo 4 — Criar o Job (Automação via Jobs & Pipelines)

1. Acesse **Workflows → Create Job**
2. Nomeie: `Pipeline_SeguroDB_Medalhao`
3. Adicione as tarefas **em sequência**:

```
[Task 1] landing_extracao
         Notebook: 01_landing_extracao
         Cluster:  segurodb-pipeline
              ↓ depends on
[Task 2] bronze_ingestao
         Notebook: 02_bronze_ingestao
         Cluster:  segurodb-pipeline
              ↓ depends on
[Task 3] silver_data_quality
         Notebook: 03_silver_data_quality
         Cluster:  segurodb-pipeline
              ↓ depends on
[Task 4] gold_modelagem_dimensional
         Notebook: 04_gold_modelagem_dimensional
         Cluster:  segurodb-pipeline
```

4. Clique em **Run Now** para executar o pipeline completo

---

## 🏛️ Arquitetura das Camadas

### 🟤 LANDING/DADOS — Dados Brutos

- **Formato:** CSV com header
- **Origem:** PostgreSQL (Supabase) via JDBC
- **Localização:** `/Volumes/main/landing/dados/<tabela>/`
- **Carga:** Full load (overwrite) a cada execução
- **Características:** Dados exatamente como extraídos da fonte, sem transformação

### 🥉 BRONZE — Ingestão Bruta (Delta Lake)

- **Formato:** Delta Lake
- **Schema:** `BRONZE`
- **Modo:** Overwrite com versionamento
- **Adições:** Colunas de metadados (`_ingestao_timestamp`, `_origem`, `_formato_origem`)
- **Benefícios:** ACID, Time Travel, Schema Enforcement

### 🥈 SILVER — Dados Tratados e Confiáveis

- **Formato:** Delta Lake
- **Schema:** `SILVER`
- **Regras de Data Quality aplicadas:**
  - Deduplicação por chave primária
  - Remoção de nulos em campos obrigatórios
  - Padronização de strings (trim + capitalização)
  - Validação de CPF (11 dígitos numéricos)
  - Validação de datas (`data_inicio < data_fim`)
  - Valores monetários não negativos
  - Padronização de enumerações (lower case)

### 🥇 GOLD — Modelagem Dimensional (Ralph Kimball)

- **Formato:** Delta Lake
- **Schema:** `GOLD`
- **Padrão:** Star Schema

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `DIM_CLIENTE` | Dimensão | Clientes com localização desnormalizada |
| `DIM_VEICULO` | Dimensão | Veículos com marca e modelo |
| `DIM_COBERTURA` | Dimensão | Tipos de cobertura com enriquecimento |
| `DIM_LOCALIDADE` | Dimensão | Municípios, estados e regiões |
| `DIM_TEMPO` | Dimensão | Calendário com atributos temporais |
| `FATO_SINISTROS` | Fato | Ocorrências com métricas e FKs |

---

## 📁 Estrutura do Repositório

```
trabalho3_databricks/
├── data/
│   └── 01_setup_supabase.sql       # Script DDL + dados para o Supabase
├── docs/
│   ├── index.md                    # Página inicial do MkDocs
│   ├── arquitetura.md              # Arquitetura Medalhão e decisões técnicas
│   ├── databricks.md               # Guia de configuração do Databricks
│   ├── pipeline.md                 # Detalhamento de cada camada
│   └── gold_kimball.md             # Modelagem Dimensional Ralph Kimball
├── notebooks/
│   ├── 01_landing_extracao.py      # Extração: Supabase → LANDING (CSV)
│   ├── 02_bronze_ingestao.py       # Ingestão: LANDING → BRONZE (Delta)
│   ├── 03_silver_data_quality.py   # Data Quality: BRONZE → SILVER (Delta)
│   └── 04_gold_modelagem_dimensional.py # Modelagem: SILVER → GOLD (Kimball)
├── .gitignore
├── mkdocs.yml
└── README.md
```

---

## 📚 Referências

- [Databricks Free Edition](https://community.cloud.databricks.com/) — Plataforma PaaS utilizada
- [Delta Lake — Documentação Oficial](https://docs.delta.io/latest/index.html) — Formato de tabelas
- [Supabase — Documentação](https://supabase.com/docs) — PostgreSQL gerenciado na nuvem
- [Ralph Kimball — The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) — Metodologia de modelagem dimensional
- [Arquitetura Medalhão — Databricks](https://www.databricks.com/glossary/medallion-architecture) — Padrão Landing/Bronze/Silver/Gold
- [Canal DataWay BR](https://www.youtube.com/@DataWayBR) — Referência do professor
- [Repositório de referência — spark-delta-minio-sqlserver](https://github.com/jlsilva01/spark-delta-minio-sqlserver) — Modelo base do professor

---

## 📜 Licença

Projeto acadêmico — SATC, Engenharia de Software, 5ª Fase, 2025.
