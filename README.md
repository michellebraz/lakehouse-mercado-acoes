# lakehouse-mercado-acoes

Pipeline de dados de mercado de ações em arquitetura medalhão com **Azure Data Factory**, **Databricks** e **Delta Lake**.

> Tickers e empresas são reais; **preços, clientes e posições são fictícios**, gerados para estudo.

```
Azure SQL (cadastro, clientes, posições) --ADF incremental--> ADLS (landing, Parquet)
Arquivo diário do pregão (CSV) --> Bronze (Auto Loader) --> Silver (validação, quarentena, MERGE) --> Gold (indicadores)
                                                                                        \--> Log de qualidade
```

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `notebooks/` | Notebooks do Databricks (formato *source*, importáveis direto) |
| `sql/` | Scripts da origem no Azure SQL / SQL Server |
| `dados/` | Os mesmos dados em CSV (plano B e upload manual) |
| `adf/` | Datasets, pipeline e trigger do Azure Data Factory em JSON |

---

## 1. Databricks Free Edition: importar os notebooks

**Opção A, mais rápida (importação manual):**
1. No menu lateral, **Workspace** → sua pasta de usuário (ou crie a pasta `lakehouse-mercado-acoes`).
2. Botão **⋮** (ou clique direito na pasta) → **Import** → **File** → arraste os 6 arquivos de `notebooks/`.
3. Eles viram notebooks porque a primeira linha é `# Databricks notebook source` (ou `-- Databricks notebook source` nos `.sql`).

**Opção B, com Git (mais profissional):** suba esta pasta para um repositório no GitHub, conecte o GitHub em *Settings → Linked accounts* e crie um **Git folder** apontando para o repositório.

**Primeira execução, manual e na ordem:**

| Notebook | O que faz | Parâmetros |
|---|---|---|
| `00_setup_e_dados` | Schemas, volume, cadastro e arquivo do pregão | `data_ref` = um dia útil, `dias` = 60 |
| `01_bronze` | Auto Loader → `bronze.cotacoes` | — |
| `02_silver` | Validação, quarentena, deduplicação, MERGE | — |
| `03_gold` | Indicadores e resumo por setor; OPTIMIZE | — |
| `04_qualidade` | Checagens e `gold.log_qualidade`; falha se houver erro | `data_ref` igual ao do 00 |
| `05_demo_entrevista` | Consultas de demonstração (fora do job) | — |

Conecte cada notebook ao compute **Serverless** (canto superior direito) antes de rodar.

**Alternativa ao 00 para as cotações:** em *Catalog → workspace → bronze → landing*, crie a pasta `cotacoes/2026-11-25/` e faça upload de `dados/cotacoes_2026-11-25.csv`. O cadastro ainda precisa do 00 (ou rode só as células 1 e 2 dele).

## 2. Databricks: criar o job `job_pregao_diario`

1. **Jobs & Pipelines** → **Create** → **Job**. Nome: `job_pregao_diario`.
2. Tarefas, todas do tipo *Notebook*, compute *Serverless*, cada uma dependendo da anterior:
   `t00_dados` → `t01_bronze` → `t02_silver` → `t03_gold` → `t04_qualidade`.
3. **Job parameters:** `data_ref` = `{{job.start_time.iso_date}}` (data da execução, preenchida automaticamente) e `dias` = `1`.
   Os parâmetros do job chegam aos notebooks como widgets.
4. Em cada tarefa: **Retries** = 1. No job: **Notifications** → e-mail em *Failure*.
5. **Schedule:** segunda a sexta, 19h30, fuso `America/Sao_Paulo`.
6. Teste com **Run now with different parameters** (`data_ref` = um dia útil). Anote o **job ID** (aparece na página do job).

## 3. Token para o ADF disparar o job (opcional)

*Settings → Developer → Access tokens → Generate new token*. Guarde o valor: ele vai no parâmetro `databricks_token` do pipeline.
Em produção, o token ficaria no **Key Vault** ou seria substituído por managed identity (com Azure Databricks).
Se a opção de token não estiver disponível na sua conta, deixe a atividade `RunJobPregao` desativada e use o agendamento do próprio job.

## 4. Azure: banco de origem

1. Crie o Azure SQL Database com a oferta gratuita (banco `db_mercado`), liberando no firewall *Allow Azure services* e o seu IP.
2. No **Query editor** do banco: rode `sql/01_criar_tabelas_e_dados.sql`; depois, sozinho, `sql/02_criar_procedure.sql`.
3. Crie uma Storage Account com **Hierarchical namespace** habilitado e um container **`landing`**.

## 5. Azure Data Factory: importar o fluxo

### 5.1 Linked services (pela interface, porque guardam credenciais)

Os JSONs usam **exatamente** estes nomes:

| Nome | Tipo | Configuração |
|---|---|---|
| `LS_AzureSql_Mercado` | Azure SQL Database | servidor, `db_mercado`, *SQL authentication*, usuário e senha → **Test connection** |
| `LS_ADLS_Mercado` | Azure Data Lake Storage Gen2 | *Account key*, selecione a storage account → **Test connection** |

### 5.2 Datasets e pipeline (colando o JSON)

1. **Author** → **+** → **Dataset** → *Azure SQL Database* → nome `DS_AzureSql_Tabela` → OK.
   Clique no botão **`{}`** (Code), apague o conteúdo, cole `adf/dataset/DS_AzureSql_Tabela.json` e confirme.
2. Mesmo processo para `DS_ADLS_Parquet` (tipo *Azure Data Lake Storage Gen2* → *Parquet*).
3. **+** → **Pipeline** → botão **`{}`** → cole `adf/pipeline/PL_Origem_Incremental.json`.
4. **Trigger:** no pipeline, *Add trigger → New/Edit*, recorrência semanal, segunda a sexta, 19h, fuso de Brasília (o JSON em `adf/trigger/` é a referência).
5. **Debug**; confira o *Output* de cada atividade. Depois **Publish all**.

*Alternativa com Git:* em *Manage → Git configuration*, conecte o GitHub com *Root folder* = `/adf`. O ADF lê as pastas `dataset/`, `pipeline/` e `trigger/`. Crie os linked services na própria branch.

### 5.3 Como o pipeline funciona

```
LkpTabelas (lista em dbo.controle_watermark)
  └─ ForEachTabela (paralelo, 3 por vez)
       LkpMax (maior updated_at atual)
        → CopyTabela (só a janela ultimo_valor < updated_at <= max_atual, para landing/<tabela>/<data>/ em Parquet)
        → SpAtualizaWatermark (só se a cópia deu certo)
→ RunJobPregao (POST /api/2.2/jobs/run-now; começa desativada)
```

### 5.4 Testes

1. 1ª execução: copia tudo (10 ativos, 200 clientes, 618 posições). Confira `landing/` e `SELECT * FROM dbo.controle_watermark`.
2. Rode `sql/03_simular_alteracoes.sql` e execute de novo: devem ser copiados **1 ativo, 1 cliente e 3 posições**.
3. Sem alterações, uma 3ª execução copia **0 linhas** (incremental funcionando).
4. Para ligar o Databricks: preencha `databricks_url`, `databricks_job_id` e `databricks_token`, clique com o botão direito em `RunJobPregao` → **Activate**.

### 5.5 Erros comuns

| Erro | Causa | Solução |
|---|---|---|
| *Cannot connect to SQL Database* no Test connection | *Allow Azure services* desligado | Ligar no Networking do servidor SQL |
| *Login failed for user* | Usuário/senha ou servidor sem `.database.windows.net` | Conferir o linked service |
| *Invalid object name 'dbo....'* | Scripts não rodados ou banco errado | Rodar `sql/01` no `db_mercado` |
| *This request is not authorized* no ADLS | Chave errada ou storage sem *Hierarchical namespace* | Refazer o linked service / criar a storage com HNS |
| Web activity com 401/403 | Token inválido ou expirado | Gerar novo token |
| Primeira execução lenta ou timeout | Banco pausado retomando | Rodar de novo após 1 minuto |

---

## Próximos passos (evolução)

Dados reais de séries históricas de cotações · calendário de pregões · eventos corporativos e preço ajustado · valor diário da carteira · SCD tipo 2 no cadastro · Lakeflow Declarative Pipelines com expectations · dashboard · CI/CD com Databricks Asset Bundles e GitHub Actions · streaming intradiário simulado.
