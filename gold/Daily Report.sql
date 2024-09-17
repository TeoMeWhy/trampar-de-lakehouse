-- Databricks notebook source
-- DBTITLE 1,Quantidade de Vagas Total
SELECT qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef = 'Geral'
AND descLinguagemVaga = 'Geral'
AND descPaisVaga = 'Geral'          
AND descMoedaVaga = 'Geral'

-- COMMAND ----------

-- DBTITLE 1,Moeda
SELECT descMoedaVaga AS `Moeda`,
       qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef = 'Geral'
AND descLinguagemVaga = 'Geral'
AND descPaisVaga = 'Geral'
AND descMoedaVaga != 'Geral'
ORDER BY qtdeVagas DESC

-- COMMAND ----------

-- DBTITLE 1,Países
SELECT descPaisVaga,
       qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef = 'Geral'
AND descLinguagemVaga = 'Geral'
AND descPaisVaga != 'Geral'
AND descMoedaVaga = 'Geral'
ORDER BY qtdeVagas DESC
LIMIT 10


-- COMMAND ----------

-- DBTITLE 1,Idioma
SELECT descLinguagemVaga AS `Idioma`,
       qtdeVagas
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef = 'Geral'
AND descLinguagemVaga != 'Geral'
AND descPaisVaga = 'Geral'
AND descMoedaVaga = 'Geral'
ORDER BY qtdeVagas DESC
LIMIT 10


-- COMMAND ----------

-- DBTITLE 1,Histórico
SELECT date(dtRef) AS `Data`,
       qtdeVagas,
       sum(qtdeVagas) over (order by dtRef ASC) AS qtdeVagasAcum
FROM gold.trampar_de_casa.cubo_lingua
WHERE dtRef != 'Geral'
AND descLinguagemVaga = 'Geral'
AND descPaisVaga = 'Geral'
AND descMoedaVaga = 'Geral'
ORDER BY dtRef ASC

