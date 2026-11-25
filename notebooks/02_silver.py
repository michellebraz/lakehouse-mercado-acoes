# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: tipagem, validação, quarentena, deduplicação e MERGE
# MAGIC **Objetivo:** uma cotação **confiável e única** por ativo e por dia de pregão.
# MAGIC
# MAGIC **Fluxo:** bronze (texto) → tipagem segura → validação contra regras de mercado e cadastro → rejeitados para a quarentena com motivo → deduplicação → `MERGE` na silver.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

CATALOGO = "workspace"
tabela_silver = f"{CATALOGO}.silver.cotacoes"
tabela_quarentena = f"{CATALOGO}.silver.cotacoes_quarentena"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tipagem segura
# MAGIC O serverless usa **modo ANSI**: um `cast` comum de texto inválido **derruba o job**.
# MAGIC `try_cast` e `try_to_timestamp` devolvem `NULL` no lugar do erro, e a validação seguinte decide o destino da linha.

# COMMAND ----------

precos = ["abertura", "maxima", "minima", "fechamento", "volume"]

tipado = spark.table(f"{CATALOGO}.bronze.cotacoes").select(
    F.expr("CAST(try_to_timestamp(data_pregao) AS DATE)").alias("data_pregao"),
    F.upper(F.trim("ticker")).alias("ticker"),                              # padroniza o ticker
    *[F.expr(f"try_cast({c} AS DECIMAL(18,2))").alias(c) for c in precos],  # DECIMAL: valor financeiro sem erro de ponto flutuante
    F.expr("try_cast(qtd_negocios AS INT)").alias("qtd_negocios"),
    "_ingestao_ts", "_arquivo",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validação
# MAGIC Regras de qualquer base de dados de mercado:
# MAGIC - data de pregão válida
# MAGIC - ativo existe no cadastro (join com `silver.ativos`; tabela pequena → **broadcast**, evita shuffle)
# MAGIC - preço de fechamento positivo
# MAGIC - máxima ≥ mínima, e fechamento dentro da faixa do dia
# MAGIC
# MAGIC `coalesce(..., False)` é importante: comparação com `NULL` dá `NULL`, e a linha sumiria dos dois lados do filtro.

# COMMAND ----------

cadastro = spark.table(f"{CATALOGO}.silver.ativos").select("ticker", F.lit(True).alias("cadastrado"))
com_cadastro = tipado.join(F.broadcast(cadastro), "ticker", "left")

ok = (F.col("data_pregao").isNotNull()
      & F.col("cadastrado")
      & (F.col("fechamento") > 0)
      & (F.col("maxima") >= F.col("minima"))
      & F.col("fechamento").between(F.col("minima"), F.col("maxima")))
regra = F.coalesce(ok, F.lit(False))

# Motivo legível da rejeição: ajuda quem for investigar (e a área de negócio)
motivo = (F.when(F.col("data_pregao").isNull(), "data inválida")
           .when(F.col("cadastrado").isNull(), "ativo fora do cadastro")
           .when(F.coalesce(F.col("fechamento"), F.lit(0)) <= 0, "preço zerado ou inválido")
           .otherwise("faixa de preço inconsistente"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quarentena
# MAGIC Os rejeitados **não são descartados**: ficam com o motivo para análise e correção na origem.
# MAGIC `overwrite` porque este notebook reprocessa a bronze inteira; assim a quarentena reflete o estado atual, sem acumular repetidos.

# COMMAND ----------

(com_cadastro.filter(~regra)
    .withColumn("motivo", motivo)
    .drop("cadastrado")
    .write.mode("overwrite")
    .saveAsTable(tabela_quarentena))

display(spark.sql(f"SELECT motivo, COUNT(*) AS linhas FROM {tabela_quarentena} GROUP BY motivo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicação
# MAGIC Chave de negócio: **(data_pregao, ticker)**. Se a mesma cotação chegou mais de uma vez,
# MAGIC `row_number` ordenado pela ingestão mais recente mantém só a última versão.

# COMMAND ----------

janela = Window.partitionBy("data_pregao", "ticker").orderBy(F.col("_ingestao_ts").desc())

validos = (com_cadastro.filter(regra)
    .drop("cadastrado")
    .withColumn("rn", F.row_number().over(janela))
    .filter("rn = 1")
    .drop("rn"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MERGE (upsert)
# MAGIC - Existe a chave → atualiza; não existe → insere.
# MAGIC - Rodar duas vezes dá o mesmo resultado (**idempotente**). É isso que permite reprocessar um dia sem medo.
# MAGIC - Na primeira execução a tabela ainda não existe: criamos vazia com o schema certo.

# COMMAND ----------

if not spark.catalog.tableExists(tabela_silver):
    validos.limit(0).write.saveAsTable(tabela_silver)

(DeltaTable.forName(spark, tabela_silver).alias("t")
    .merge(validos.alias("s"), "t.data_pregao = s.data_pregao AND t.ticker = s.ticker")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# Métricas da operação (linhas inseridas/atualizadas) ficam no histórico do Delta
display(spark.sql(f"DESCRIBE HISTORY {tabela_silver} LIMIT 1").select("version", "operation", "operationMetrics"))
