-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Roteiro de demonstração (não entra no job)
-- MAGIC Consultas prontas para mostrar o projeto na entrevista, na ordem do fluxo.

-- COMMAND ----------

-- MAGIC %md ## 1. Reconciliação entre camadas
-- MAGIC A bronze tem mais linhas (duplicados e inválidos); a silver tem 1 linha por ativo e pregão.

-- COMMAND ----------

SELECT 'bronze' AS camada, COUNT(*) AS linhas FROM workspace.bronze.cotacoes
UNION ALL SELECT 'quarentena', COUNT(*) FROM workspace.silver.cotacoes_quarentena
UNION ALL SELECT 'silver', COUNT(*) FROM workspace.silver.cotacoes
UNION ALL SELECT 'gold (indicadores)', COUNT(*) FROM workspace.gold.indicadores_diarios;

-- COMMAND ----------

-- MAGIC %md ## 2. O que foi rejeitado e por quê

-- COMMAND ----------

SELECT motivo, ticker, data_pregao, fechamento, maxima, minima, _arquivo
FROM workspace.silver.cotacoes_quarentena
ORDER BY motivo;

-- COMMAND ----------

-- MAGIC %md ## 3. Histórico do Delta e time travel
-- MAGIC Cada escrita gera uma versão. Dá para consultar ou restaurar uma versão anterior (`RESTORE TABLE ... TO VERSION AS OF n`).

-- COMMAND ----------

DESCRIBE HISTORY workspace.silver.cotacoes;

-- COMMAND ----------

SELECT COUNT(*) AS linhas_na_versao_1 FROM workspace.silver.cotacoes VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md ## 4. Indicadores de um ativo

-- COMMAND ----------

SELECT data_pregao, fechamento,
       ROUND(retorno_dia * 100, 2) AS retorno_pct,
       ROUND(media_movel_20, 2) AS media_movel_20,
       ROUND(volatilidade_anual_20 * 100, 1) AS volatilidade_pct
FROM workspace.gold.indicadores_diarios
WHERE ticker = 'PETR4'
ORDER BY data_pregao DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 5. Saúde do pipeline

-- COMMAND ----------

SELECT * FROM workspace.gold.log_qualidade ORDER BY execucao_ts DESC;
