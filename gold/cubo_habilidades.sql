WITH tb_cubo AS (

       SELECT 
              date(t1.dtCriacao) AS dtRef,
              COALESCE(t3.descNomeNormalizadoHabilidade, 'NÃO CONSTA / NÃO INFORMADO') AS descHabilidade,
              count(distinct t1.idVaga) AS qtdeVaga

       FROM silver.trampar_de_casa.vagas AS t1

       LEFT JOIN silver.trampar_de_casa.habilidades_vagas AS t2
       ON t1.idVaga = t2.idVaga

       LEFT JOIN silver.trampar_de_casa.habilidades AS t3
       ON t2.idHabilidade = t3.idHabilidade

       GROUP BY 1,2 WITH CUBE
       ORDER BY 1,2

)

SELECT 
       coalesce(dtRef, 'GERAL') AS dtRef,
       coalesce(descHabilidade, 'GERAL') AS descHabilidade,
       qtdeVaga

 FROM tb_cubo