# ⚙️ Configuração do Databricks

Guia passo a passo para configurar o ambiente e reproduzir o projeto do zero.

---

## Pré-requisitos

!!! note "Tudo na nuvem — sem instalação local obrigatória"
    Você precisa apenas de um navegador web. O processamento ocorre 100% no Databricks.

- [ ] Conta no **[Databricks Community Edition](https://community.cloud.databricks.com/)** (gratuita)
- [ ] Conta no **[Supabase](https://supabase.com/)** (gratuita)
- [ ] Git instalado localmente (para clonar o repositório)

---

## Passo 1 — Configurar o Banco de Dados no Supabase

### 1.1 Criar o projeto

1. Acesse [supabase.com](https://supabase.com) e faça login
2. Clique em **New Project**
3. Preencha:
   - **Name:** `segurodb`
   - **Database Password:** escolha uma senha forte e **anote**
   - **Region:** South America (São Paulo) — mais próximo
4. Clique em **Create new project** e aguarde ~2 minutos

### 1.2 Executar o script de setup

1. No painel do Supabase, clique em **SQL Editor** (menu lateral)
2. Clique em **New query**
3. Copie e cole todo o conteúdo de [`data/01_setup_supabase.sql`](https://github.com/Isaac-Alexsander/trabalho3_databricks/blob/main/data/01_setup_supabase.sql)
4. Clique em **Run** (▶️)
5. Verifique a mensagem: `Setup concluído! Tabelas e dados carregados.`

### 1.3 Coletar as credenciais JDBC

Navegue até: **Project Settings → Database → Connection string → JDBC**

Copie e guarde:

```
Host:     db.xxxxxxxxxxxxxxxx.supabase.co
Port:     5432
Database: postgres
User:     postgres.xxxxxxxxxxxxxxxx
Password: <sua senha do projeto>
```

!!! warning "Atenção com o usuário"
    Em projetos recentes do Supabase, o usuário JDBC tem o formato `postgres.PROJECT_ID`.
    Verifique exatamente o valor exibido na aba JDBC.

---

## Passo 2 — Configurar o Cluster Databricks

### 2.1 Criar o cluster

1. Acesse [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Vá em **Compute** → **Create compute**
3. Configure:

| Campo | Valor |
|-------|-------|
| Cluster name | `segurodb-pipeline` |
| Databricks Runtime | `14.3 LTS (Spark 3.5, Scala 2.12)` |
| Terminate after | `30 minutes` de inatividade |

4. Clique em **Create compute**

### 2.2 Instalar o driver PostgreSQL

Com o cluster criado, na aba **Libraries**:

1. Clique em **Install new**
2. Selecione **Maven**
3. Em **Coordinates**, cole:
   ```
   org.postgresql:postgresql:42.7.3
   ```
4. Clique em **Install**
5. Aguarde o status ficar **Installed** ✅

---

## Passo 3 — Criar os Volumes (Unity Catalog)

No Databricks, abra um novo notebook e execute:

```sql
-- Cria estrutura de volumes para o Landing
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.landing;
CREATE VOLUME  IF NOT EXISTS main.landing.dados;
```

!!! info "O que são Volumes?"
    Volumes são sistemas de arquivos gerenciados pelo Unity Catalog do Databricks,
    acessíveis via caminho `/Volumes/main/landing/dados/`. São o equivalente
    a buckets S3 do MinIO (usado no Trabalho 2), mas gerenciados nativamente.

---

## Passo 4 — Configurar as Credenciais (Secrets)

### Opção A — Databricks Secrets (recomendado para produção)

Instale a [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) e execute:

```bash
# Configura autenticação
databricks configure --token

# Cria o scope de secrets
databricks secrets create-scope segurodb

# Adiciona as credenciais
databricks secrets put-secret segurodb host
# (insira o host do Supabase quando solicitado)

databricks secrets put-secret segurodb user
# (insira o user JDBC do Supabase)

databricks secrets put-secret segurodb password
# (insira a senha do Supabase)
```

No notebook, as credenciais são acessadas com:
```python
host     = dbutils.secrets.get(scope="segurodb", key="host")
user     = dbutils.secrets.get(scope="segurodb", key="user")
password = dbutils.secrets.get(scope="segurodb", key="password")
```

### Opção B — Variáveis diretas (apenas para testes locais)

!!! danger "Nunca faça commit de credenciais!"
    Use esta opção apenas para testar no Databricks interativamente.
    Jamais salve o notebook com as credenciais preenchidas no repositório.

```python
# Substitua os valores no notebook 01 para testes
SUPABASE_HOST     = "db.xxxxxxxxxxxxxxxx.supabase.co"
SUPABASE_USER     = "postgres.xxxxxxxxxxxxxxxx"
SUPABASE_PASSWORD = "sua_senha_aqui"
```

---

## Passo 5 — Importar os Notebooks

1. No Databricks, acesse **Workspace → seu usuário**
2. Crie uma pasta: **trabalho3_databricks**
3. Clique em **Import** (⋮ → Import)
4. Importe **um por um** os arquivos da pasta `notebooks/`:

```
01_landing_extracao.py
02_bronze_ingestao.py
03_silver_data_quality.py
04_gold_modelagem_dimensional.py
```

5. Associe cada notebook ao cluster `segurodb-pipeline`

---

## Passo 6 — Criar o Job (Jobs & Pipelines)

Este é o requisito de **automação** do trabalho — os notebooks devem ser encadeados.

1. Acesse **Workflows → Create job**
2. **Job name:** `Pipeline_SeguroDB_Medalhao`

### Configurar as Tasks em sequência:

**Task 1:**
- Task name: `01_landing`
- Type: `Notebook`
- Source: Workspace → `01_landing_extracao`
- Cluster: `segurodb-pipeline`

**Task 2:**
- Task name: `02_bronze`
- Type: `Notebook`
- Source: Workspace → `02_bronze_ingestao`
- Cluster: `segurodb-pipeline`
- **Depends on:** `01_landing` ← ⚠️ obrigatório

**Task 3:**
- Task name: `03_silver`
- Type: `Notebook`
- Source: Workspace → `03_silver_data_quality`
- Cluster: `segurodb-pipeline`
- **Depends on:** `02_bronze`

**Task 4:**
- Task name: `04_gold`
- Type: `Notebook`
- Source: Workspace → `04_gold_modelagem_dimensional`
- Cluster: `segurodb-pipeline`
- **Depends on:** `03_silver`

3. Clique em **Run Now** 🚀

O DAG do job ficará assim:

```
01_landing → 02_bronze → 03_silver → 04_gold
```

---

## Verificação Final

Após a execução do Job, verifique os schemas criados:

```sql
SHOW SCHEMAS;
-- Deve exibir: landing, bronze, silver, gold

SHOW TABLES IN BRONZE;
-- Deve exibir as 11 tabelas

SHOW TABLES IN SILVER;
-- Deve exibir as 11 tabelas tratadas

SHOW TABLES IN GOLD;
-- Deve exibir: DIM_CLIENTE, DIM_VEICULO, DIM_COBERTURA,
--              DIM_LOCALIDADE, DIM_TEMPO, FATO_SINISTROS
```

!!! success "Pipeline funcionando!"
    Se todos os schemas e tabelas estiverem presentes, o pipeline foi executado com sucesso. ✅

---

## Solução de Problemas

### Erro: `ClassNotFoundException: org.postgresql.Driver`
**Causa:** Driver PostgreSQL não instalado no cluster.
**Solução:** Vá em **Compute → Libraries → Install New → Maven** e instale `org.postgresql:postgresql:42.7.3`. Reinicie o cluster.

### Erro: `Connection refused` ou timeout no JDBC
**Causa:** Endereço/porta do Supabase incorretos, ou SSL não habilitado.
**Solução:** Verifique se a URL JDBC contém `?sslmode=require`. Copie o host exatamente do painel Supabase (Project Settings → Database).

### Erro: `Secret does not exist`
**Causa:** Secret scope não criado ou nome de chave incorreto.
**Solução:** Use a Opção B (variáveis diretas) para testes, ou recrie o scope com `databricks secrets create-scope segurodb`.

### Erro: `Volume not found`
**Causa:** Volume Unity Catalog não criado.
**Solução:** Execute o SQL do Passo 3 em um notebook antes de rodar o pipeline.

---

## Referências

- [Databricks Community Edition](https://community.cloud.databricks.com/) — Plataforma gratuita
- [Supabase JDBC Connection](https://supabase.com/docs/guides/database/connecting-to-postgres#jdbc) — Documentação de conexão
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html) — Gerenciamento seguro de credenciais
- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html) — Armazenamento gerenciado
- [Databricks Jobs](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html) — Automação de pipelines
