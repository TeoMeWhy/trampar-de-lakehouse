-- Databricks notebook source
-- DBTITLE 1,WIDGETS
-- MAGIC %python
-- MAGIC
-- MAGIC import datetime
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC now = datetime.datetime.now().strftime("%Y-%m-%d")
-- MAGIC init = (spark.table("gold.trampar_de_casa.cubo_lingua")
-- MAGIC              .select(F.min("dtRef"))
-- MAGIC              .collect()[0][0])
-- MAGIC
-- MAGIC dbutils.widgets.text("02_date_end", now, "Data fim")
-- MAGIC dbutils.widgets.text("01_date_start", init, "Data início")

-- COMMAND ----------

-- DBTITLE 1,Quantidade de Vagas Total
SELECT sum(qtdeVagas) AS qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga = 'GERAL'

-- COMMAND ----------

-- DBTITLE 1,Histórico Diário
SELECT date(dtRef) AS `Data`,
       qtdeVagas,
       sum(qtdeVagas) over (order by dtRef ASC) AS qtdeVagasAcum

FROM gold.trampar_de_casa.cubo_lingua

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga = 'GERAL'

ORDER BY dtRef ASC

-- COMMAND ----------

-- DBTITLE 1,Histórico Mensal
with tb_month AS (

  SELECT date(date_trunc('month', date(dtRef))) AS `Data`,
        sum(qtdeVagas) AS qtdeVagas

  FROM gold.trampar_de_casa.cubo_lingua

  WHERE dtRef >= '${01_date_start}'
  AND dtRef <= '${02_date_end}'
  AND descLinguagemVaga = 'GERAL'
  AND descPaisVaga = 'GERAL'
  AND descMoedaVaga = 'GERAL'

  GROUP BY ALL
  ORDER BY 1 ASC

)

SELECT *,
       SUM(qtdeVagas) OVER (ORDER BY Data) AS  qtdeVagasAcum

FROM tb_month

-- COMMAND ----------

-- DBTITLE 1,Moeda
SELECT descMoedaVaga AS `Moeda`,
       sum(qtdeVagas) AS qtdeVagas

FROM gold.trampar_de_casa.cubo_lingua

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga != 'GERAL'

GROUP BY descMoedaVaga

ORDER BY qtdeVagas DESC


-- COMMAND ----------

-- DBTITLE 1,Moeda Histórico
SELECT date(dtRef) AS dtRef,
       descMoedaVaga AS `Moeda`,
       qtdeVagas AS qtdeVagas,
       sum(qtdeVagas) OVER (PARTITION BY descMoedaVaga ORDER BY date(dtRef)) AS qtdeVagasAcum


FROM gold.trampar_de_casa.cubo_lingua

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga != 'GERAL'


ORDER BY dtRef, qtdeVagas DESC

-- COMMAND ----------

-- DBTITLE 1,Países
SELECT descPaisVaga,
       sum(qtdeVagas) AS qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga != 'GERAL'
AND descMoedaVaga = 'GERAL'

GROUP BY descPaisVaga

ORDER BY qtdeVagas DESC

LIMIT 10


-- COMMAND ----------

-- DBTITLE 1,Países Histórico
WITH tb_most AS (

  SELECT descPaisVaga,
        sum(qtdeVagas) AS qtdeVagas
  FROM gold.trampar_de_casa.cubo_lingua

  WHERE dtRef >= '${01_date_start}'
  AND dtRef <= '${02_date_end}'
  AND descLinguagemVaga = 'GERAL'
  AND descPaisVaga != 'GERAL'
  AND descMoedaVaga = 'GERAL'

  GROUP BY descPaisVaga

  ORDER BY qtdeVagas DESC

  LIMIT 10
)

SELECT date(dtRef),
       descPaisVaga AS `País`,
       qtdeVagas AS qtdeVagas,
       SUM(qtdeVagas) OVER (PARTITION BY descPaisVaga ORDER BY dtRef) AS qtdeVagasAcum

FROM gold.trampar_de_casa.cubo_lingua

WHERE date(dtRef) >= '${01_date_start}'
AND date(dtRef) <= '${02_date_end}'
AND descLinguagemVaga = 'GERAL'
AND descPaisVaga IN (select descPaisVaga from tb_most)
AND descMoedaVaga = 'GERAL'

ORDER BY qtdeVagas DESC

-- COMMAND ----------

-- DBTITLE 1,Idioma
SELECT descLinguagemVaga AS `Idioma`,
       sum(qtdeVagas) AS qtdeVagas

FROM gold.trampar_de_casa.cubo_lingua

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descLinguagemVaga != 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga = 'GERAL'

GROUP BY descLinguagemVaga

ORDER BY qtdeVagas DESC

-- COMMAND ----------

-- DBTITLE 1,Idioma Históëico
SELECT date(dtRef) AS dtRef,
       descLinguagemVaga AS `Idioma`,
       qtdeVagas AS qtdeVagas,
       sum(qtdeVagas) OVER (PARTITION BY descLinguagemVaga ORDER BY dtRef) AS qtdeVagasAcum

FROM gold.trampar_de_casa.cubo_lingua

WHERE date(dtRef) >= '${01_date_start}'
AND date(dtRef) <= '${02_date_end}'
AND descLinguagemVaga != 'GERAL'
AND descPaisVaga = 'GERAL'
AND descMoedaVaga = 'GERAL'

ORDER BY qtdeVagas DESC

-- COMMAND ----------

-- DBTITLE 1,Habilidades
SELECT descHabilidade,
       sum(qtdeVaga) AS qtdeVaga

FROM gold.trampar_de_casa.cubo_habilidades

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descHabilidade != 'GERAL'

GROUP BY descHabilidade

ORDER BY qtdeVaga DESC
LIMIT 21

-- COMMAND ----------

-- DBTITLE 1,HABILIDADES HISTORICO
WITH tb_most AS (

SELECT descHabilidade,
       sum(qtdeVaga) AS qtdeVaga

FROM gold.trampar_de_casa.cubo_habilidades

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descHabilidade != 'GERAL'
AND descHabilidade != 'NÃO CONSTA / NÃO INFORMADO'

GROUP BY descHabilidade

ORDER BY 2 DESC
LIMIT 5

)

SELECT date(t1.dtRef) AS dtRef,
       t1.descHabilidade AS `Habilidade`,
       t1.qtdeVaga

FROM gold.trampar_de_casa.cubo_habilidades AS t1

WHERE dtRef >= '${01_date_start}'
AND dtRef <= '${02_date_end}'
AND descHabilidade IN (SELECT descHabilidade FROM tb_most)

ORDER BY dtRef ASC
