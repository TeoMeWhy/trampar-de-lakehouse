SELECT id AS idVaga,
       topicId AS idTopico, -- 
       url AS urlVaga,
       description AS descVaga,
       title AS descTituloVaga,
       company AS descEmpresa,
       language AS descLinguagemVaga,

       CASE 
            WHEN country IN ('Global', 'International') THEN 'International'
            WHEN country IN ('Paraná') THEN 'Brasil'
            WHEN country IS NULL AND language = 'Portuguese' THEN 'Brasil'
            WHEN country IS NULL AND language != 'Portuguese' THEN 'Internacional'
            ELSE country
       END AS descPaisVaga,
       
       currency AS descMoedaVaga,
       salary AS vlSalario,
       CASE WHEN ready = 1 THEN 1 ELSE 0 END AS descPronto, -- vaga disponível no site
       minimumYears AS vlExpMinima, -- Se exige exp mínima
       updatedAt AS dtAtualizacao,
       createdAt AS dtCriacao

FROM bronze.trampar_de_casa.roles