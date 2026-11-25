-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold: indicadores de mercado
-- MAGIC **Objetivo:** tabelas prontas para análise e BI.
-- MAGIC - `indicadores_diarios`: retorno do dia, média móvel de 20 pregões e volatilidade anualizada, por ativo
-- MAGIC - `resumo_setor_dia`: volume e retorno médio por setor
-- MAGIC
-- MAGIC Aqui as tabelas são recalculadas inteiras (`CREATE OR REPLACE`), o que é simples e suficiente para este volume.
-- MAGIC Evolução: cálculo incremental com Change Data Feed da silver.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Indicadores por ativo (window functions)
-- MAGIC - `LAG`: fechamento do pregão anterior do mesmo ativo → retorno do dia
-- MAGIC - `ROWS BETWEEN 19 PRECEDING AND CURRENT ROW`: janela dos últimos 20 pregões (linhas, não dias corridos)
-- MAGIC - `STDDEV(retorno) * SQRT(252)`: volatilidade anualizada (cerca de 252 pregões por ano)

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.indicadores_diarios AS
WITH base AS (
  SELECT c.data_pregao, c.ticker, a.setor, c.fechamento, c.volume,
         c.fechamento / LAG(c.fechamento) OVER (PARTITION BY c.ticker ORDER BY c.data_pregao) - 1 AS retorno_dia
  FROM workspace.silver.cotacoes c
  JOIN workspace.silver.ativos a ON a.ticker = c.ticker
)
SELECT *,
  AVG(fechamento) OVER (PARTITION BY ticker ORDER BY data_pregao
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS media_movel_20,
  STDDEV(retorno_dia) OVER (PARTITION BY ticker ORDER BY data_pregao
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS volatilidade_anual_20
FROM base;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Resumo por setor
-- MAGIC `GROUP BY ALL` agrupa por todas as colunas não agregadas (atalho do Databricks SQL).

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.resumo_setor_dia AS
SELECT data_pregao, setor,
       COUNT(*)          AS qtd_ativos,
       SUM(volume)       AS volume_total,
       AVG(retorno_dia)  AS retorno_medio
FROM workspace.gold.indicadores_diarios
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Performance da silver
-- MAGIC - **Liquid clustering** (`CLUSTER BY`) organiza os arquivos por ticker e data, as colunas mais filtradas. Substitui particionamento e Z-ORDER com mais flexibilidade.
-- MAGIC - `OPTIMIZE` compacta arquivos pequenos e aplica o clustering.
-- MAGIC Os dois comandos são idempotentes.

-- COMMAND ----------

ALTER TABLE workspace.silver.cotacoes CLUSTER BY (ticker, data_pregao);

-- COMMAND ----------

OPTIMIZE workspace.silver.cotacoes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Consulta de exemplo: 3 maiores altas do último pregão
-- MAGIC `QUALIFY` filtra pelo resultado de uma window function sem precisar de subconsulta.

-- COMMAND ----------

SELECT data_pregao, ticker, setor, ROUND(retorno_dia * 100, 2) AS alta_pct
FROM workspace.gold.indicadores_diarios
WHERE data_pregao = (SELECT MAX(data_pregao) FROM workspace.gold.indicadores_diarios)
QUALIFY DENSE_RANK() OVER (ORDER BY retorno_dia DESC) <= 3
ORDER BY alta_pct DESC;
