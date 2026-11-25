# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Qualidade e log de execução
# MAGIC **Objetivo:** o pipeline **falhar de forma explícita** quando o dado estiver errado, em vez de publicar número errado em silêncio.
# MAGIC
# MAGIC Checagens:
# MAGIC - **Unicidade:** nenhuma chave (data_pregao, ticker) repetida na silver
# MAGIC - **Completude:** todos os ativos do cadastro têm cotação no pregão de `data_ref`
# MAGIC
# MAGIC As métricas vão para `gold.log_qualidade` antes da decisão, então ficam registradas mesmo quando o job falha.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("data_ref", "2026-11-25", "Data do pregão (AAAA-MM-DD)")
data_ref = dbutils.widgets.get("data_ref")
CATALOGO = "workspace"

# COMMAND ----------

silver = spark.table(f"{CATALOGO}.silver.cotacoes")

total = silver.count()
duplicados = total - silver.select("data_pregao", "ticker").distinct().count()
ativos_no_dia = (silver.filter(F.col("data_pregao") == F.lit(data_ref).cast("date"))
                       .select("ticker").distinct().count())
ativos_esperados = spark.table(f"{CATALOGO}.silver.ativos").count()
quarentena = spark.table(f"{CATALOGO}.silver.cotacoes_quarentena").count()

print(f"total={total} duplicados={duplicados} ativos_no_dia={ativos_no_dia}/{ativos_esperados} quarentena={quarentena}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log de qualidade
# MAGIC Histórico de cada execução: base para alertas, dashboards de saúde do pipeline e para responder "desde quando isso acontece?".

# COMMAND ----------

(spark.createDataFrame(
        [(data_ref, total, duplicados, ativos_no_dia, ativos_esperados, quarentena)],
        "data_ref STRING, total LONG, duplicados LONG, ativos_no_dia LONG, ativos_esperados LONG, quarentena LONG")
    .withColumn("execucao_ts", F.current_timestamp())
    .write.mode("append")
    .saveAsTable(f"{CATALOGO}.gold.log_qualidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisão
# MAGIC Lançar exceção marca a tarefa como **falha**: o job dispara o alerta configurado e quem consome a gold sabe que o dia não está confiável.

# COMMAND ----------

erros = []
if duplicados > 0:
    erros.append(f"{duplicados} chave(s) duplicada(s)")
if ativos_no_dia < ativos_esperados:
    erros.append(f"faltam cotações: {ativos_no_dia} de {ativos_esperados} ativos em {data_ref}")

if erros:
    raise Exception("Falha de qualidade: " + "; ".join(erros))

print("Qualidade OK")
