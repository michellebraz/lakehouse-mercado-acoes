# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup e geração de dados
# MAGIC **Projeto:** lakehouse-mercado-acoes
# MAGIC
# MAGIC **Objetivo:** criar a estrutura do lakehouse (schemas e volume), o cadastro de ativos e simular o arquivo diário de cotações do pregão.
# MAGIC
# MAGIC **Em produção este notebook não existiria:** o cadastro viria do SQL Server pelo ADF, e o arquivo de cotações chegaria do fornecedor de dados de mercado na área de pouso (ADLS).
# MAGIC
# MAGIC **Parâmetros (widgets):**
# MAGIC - `data_ref`: data do pregão, formato AAAA-MM-DD, sempre dia útil
# MAGIC - `dias`: quantos pregões gerar terminando em `data_ref` (60 na primeira execução, 1 nas seguintes)

# COMMAND ----------

# Widgets são os parâmetros do notebook.
# Quando o notebook roda dentro de um job, os parâmetros do job preenchem esses valores automaticamente.
dbutils.widgets.text("data_ref", "2026-11-25", "Data do pregão (AAAA-MM-DD)")
dbutils.widgets.text("dias", "60", "Pregões a gerar")

import datetime as dt

CATALOGO = "workspace"   # catálogo padrão do Free Edition
data_ref = dt.date.fromisoformat(dbutils.widgets.get("data_ref"))
dias = int(dbutils.widgets.get("dias"))

# Validação de entrada: falhar cedo e com mensagem clara é boa prática.
# Sábado e domingo não têm pregão (feriados ficam para a evolução do projeto: calendário de pregões).
if data_ref.weekday() >= 5:
    raise ValueError(f"{data_ref} é fim de semana: não há pregão. Use um dia útil em data_ref.")

print(f"Gerando {dias} pregão(ões) até {data_ref}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Estrutura no Unity Catalog
# MAGIC Unity Catalog organiza tudo em `catálogo.schema.tabela`, com permissões e linhagem.
# MAGIC Um schema por camada deixa claro o contrato de cada uma. O **volume** é a área governada para arquivos brutos.
# MAGIC `IF NOT EXISTS` torna o comando idempotente: rodar de novo não dá erro.

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cadastro de ativos (dado de referência)
# MAGIC Usado na silver para validar se o ticker existe e na gold para trazer o setor.
# MAGIC Em produção vem do SQL Server (tabela `dbo.ativos`) pela carga incremental do ADF.

# COMMAND ----------

cadastro = [
    ("PETR4", "Petrobras", "Petróleo e gás", "PN"),
    ("VALE3", "Vale", "Mineração", "ON"),
    ("ITUB4", "Itaú Unibanco", "Bancos", "PN"),
    ("BBDC4", "Bradesco", "Bancos", "PN"),
    ("BBAS3", "Banco do Brasil", "Bancos", "ON"),
    ("WEGE3", "WEG", "Bens industriais", "ON"),
    ("ABEV3", "Ambev", "Bebidas", "ON"),
    ("MGLU3", "Magazine Luiza", "Varejo", "ON"),
    ("RENT3", "Localiza", "Locação de veículos", "ON"),
    ("SUZB3", "Suzano", "Papel e celulose", "ON"),
]
(spark.createDataFrame(cadastro, "ticker STRING, empresa STRING, setor STRING, tipo STRING")
     .write.mode("overwrite")                 # overwrite: tabela pequena de referência, sempre substituída inteira
     .saveAsTable(f"{CATALOGO}.silver.ativos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulação do arquivo do pregão
# MAGIC Preços gerados por um passeio aleatório **determinístico**: a semente depende do ticker e da data,
# MAGIC então a mesma data gera sempre o mesmo preço, em qualquer execução. Isso evita histórico inconsistente.
# MAGIC
# MAGIC **Problemas inseridos de propósito** (para tratar na silver e conversar na entrevista):
# MAGIC 1. arquivo reenviado (linhas duplicadas)
# MAGIC 2. ativo fora do cadastro (`XPTO3`)
# MAGIC 3. preço zerado
# MAGIC 4. máxima menor que a mínima
# MAGIC 5. data inválida

# COMMAND ----------

import random, math

preco_inicial = {"PETR4": 38, "VALE3": 61, "ITUB4": 36, "BBDC4": 15, "BBAS3": 22,
                 "WEGE3": 45, "ABEV3": 13, "MGLU3": 9, "RENT3": 42, "SUZB3": 55}

# Calendário de dias úteis desde o início do ano até data_ref
calendario, d = [], dt.date(2026, 1, 2)
while d <= data_ref:
    if d.weekday() < 5:
        calendario.append(d)
    d += dt.timedelta(days=1)
gerar = set(calendario[-dias:])      # só os últimos N pregões vão para o arquivo

linhas = []
for ticker, preco in preco_inicial.items():
    for dia in calendario:
        rnd = random.Random(f"{ticker}-{dia}")                     # semente fixa por ticker + data
        abertura = round(preco * (1 + rnd.uniform(-0.005, 0.005)), 2)
        preco = round(preco * math.exp(rnd.gauss(0, 0.02)), 2)     # variação diária ~2%
        if dia in gerar:
            maxima = round(max(abertura, preco) * (1 + rnd.uniform(0, 0.01)), 2)
            minima = round(min(abertura, preco) * (1 - rnd.uniform(0, 0.01)), 2)
            negocios = rnd.randint(5000, 60000)
            linhas.append((str(dia), ticker, abertura, maxima, minima, preco,
                           round(negocios * 100 * preco, 2), negocios))

# Problemas reais, de propósito
linhas += linhas[:15]                                                       # 1. arquivo reenviado
linhas.append((str(data_ref), "XPTO3", 10.0, 11.0, 9.0, 10.5, 1e5, 10))    # 2. ativo fora do cadastro
linhas.append((str(data_ref), "PETR4", 0.0, 0.0, 0.0, 0.0, 0.0, 0))        # 3. preço zerado
linhas.append((str(data_ref), "VALE3", 60.0, 58.0, 61.0, 59.0, 1e6, 100))  # 4. máxima < mínima
linhas.append(("2026-13-01", "ITUB4", 36.0, 37.0, 35.0, 36.5, 1e6, 100))   # 5. data inválida

print(f"{len(linhas)} linhas geradas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação na área de pouso
# MAGIC Uma pasta por data de referência, como um fornecedor entregaria.
# MAGIC `coalesce(1)` gera um único arquivo CSV (o volume é pequeno; em produção não forçaríamos isso).

# COMMAND ----------

schema = ("data_pregao STRING, ticker STRING, abertura DOUBLE, maxima DOUBLE, minima DOUBLE, "
          "fechamento DOUBLE, volume DOUBLE, qtd_negocios INT")
destino = f"/Volumes/{CATALOGO}/bronze/landing/cotacoes/{data_ref}"

(spark.createDataFrame(linhas, schema)
     .coalesce(1)
     .write.mode("overwrite")
     .option("header", True)
     .csv(destino))

display(dbutils.fs.ls(destino))
