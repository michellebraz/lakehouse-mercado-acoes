# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingestão incremental com Auto Loader
# MAGIC **Objetivo:** carregar o arquivo do pregão **exatamente como chegou**, sem regra de negócio, só com metadados de rastreio.
# MAGIC
# MAGIC **Por que Auto Loader:**
# MAGIC - O **checkpoint** guarda quais arquivos já foram lidos → rodar de novo não duplica (idempotência).
# MAGIC - `trigger(availableNow=True)` processa o que chegou e para, como um batch (bom para job agendado e mais barato que streaming contínuo).
# MAGIC - Na bronze tudo chega como **texto** (CSV sem inferência de tipos). A tipagem é responsabilidade da silver.
# MAGIC - A coluna `_rescued_data` guarda dados que não couberam no schema (ex.: coluna nova) em vez de quebrar o job.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
origem = f"/Volumes/{CATALOGO}/bronze/landing/cotacoes/"
checkpoint = f"/Volumes/{CATALOGO}/bronze/landing/_checkpoints/cotacoes_bronze"
tabela_bronze = f"{CATALOGO}.bronze.cotacoes"

# COMMAND ----------

consulta = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.schemaLocation", checkpoint)   # onde o Auto Loader guarda o schema e sua evolução
    .load(origem)
    # Metadados de rastreio: quando entrou e de qual arquivo veio.
    # Essenciais para troubleshooting e reconciliação ("de onde veio essa linha?").
    .withColumn("_ingestao_ts", F.current_timestamp())
    .withColumn("_arquivo", F.col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(tabela_bronze))

consulta.awaitTermination()   # espera terminar para a próxima tarefa do job só começar com a bronze pronta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência: linhas por arquivo
# MAGIC Se o mesmo arquivo for reenviado com outro nome, ele entra de novo aqui. É esperado: a deduplicação acontece na silver.

# COMMAND ----------

display(spark.sql(f"""
  SELECT _arquivo, COUNT(*) AS linhas, MAX(_ingestao_ts) AS ingerido_em
  FROM {tabela_bronze}
  GROUP BY _arquivo
  ORDER BY ingerido_em DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Opcional) Reprocessar a bronze do zero
# MAGIC Útil em troubleshooting. Descomente e rode **só se quiser recomeçar**: apaga a tabela e o checkpoint,
# MAGIC e na próxima execução o Auto Loader lê todos os arquivos de novo.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {tabela_bronze}")
# dbutils.fs.rm(checkpoint, True)
