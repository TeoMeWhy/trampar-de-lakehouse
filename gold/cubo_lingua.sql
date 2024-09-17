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
       coalesce(dtRef, 'Geral') AS dtRef,
       coalesce(descLinguagemVaga, 'Geral') AS descLinguagemVaga,
       coalesce(descPaisVaga, 'Geral') AS descPaisVaga,
       coalesce(descMoedaVaga, 'Geral') AS descMoedaVaga,
       qtdeVagas
FROM tb_cubo