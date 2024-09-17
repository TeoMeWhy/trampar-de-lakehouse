WITH tb_cubo AS (

   SELECT 
       date(dtCriacao) AS dtRef,
       descLinguagemVaga,
       descPaisVaga,
       descMoedaVaga,
       COUNT(idVaga) AS qtdeVagas
   FROM silver.trampar_de_casa.vagas
   GROUP BY 1,2,3,4 WITH CUBE
   ORDER BY 1,2,3,4

)

SELECT 
       coalesce(dtRef, 'GERAL') AS dtRef,
       coalesce(descLinguagemVaga, 'GERAL') AS descLinguagemVaga,
       coalesce(descPaisVaga, 'GERAL') AS descPaisVaga,
       coalesce(descMoedaVaga, 'GERAL') AS descMoedaVaga,
       qtdeVagas
FROM tb_cubo