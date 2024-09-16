WITH tb_first AS (

    SELECT id AS idVaga,
          topicId AS idTopico,
          url AS urlVaga,
          description AS descVaga,
          title AS descTituloVaga,
          company AS descEmpresa,
          language AS descLinguagemVaga,

          CASE 
                WHEN country IN ('Global', 'International', 'Mundial') THEN 'International'
                WHEN country IN ('Paraná',
                                'Porto Alegre-RS',
                                'Bahia',
                                'Barueri - SP',
                                'São Paulo',
                                'Sorocaba / SP',
                                'Rio de Janeiro',
                                'Brazil',
                                'São Paulo, SP',
                                'Nova Lima - MG') THEN 'Brasil'
                WHEN country IS NULL AND language = 'Portuguese' THEN 'Brasil'
                WHEN country IS NULL AND language != 'Portuguese' THEN 'Internacional'
                ELSE country
          END AS descPaisVaga,
          
          CASE
                WHEN currency IN ('Real', 'R$', 'BRL') THEN 'BRL'
                WHEN currency IN ('U$D','U$','USD') THEN 'USD'
                ELSE currency            
          END AS descMoedaVaga,
          
          salary AS vlSalario,
          CASE WHEN ready = 1 THEN 1 ELSE 0 END AS descPronto, -- vaga disponível no site
          minimumYears AS vlExpMinima, -- Se exige exp mínima
          updatedAt AS dtAtualizacao,
          createdAt AS dtCriacao

    FROM bronze.trampar_de_casa.roles

)

SELECT 
    idVaga,
    idTopico,
    urlVaga,
    descVaga,
    descTituloVaga,
    descEmpresa,
    descLinguagemVaga,
    descPaisVaga,

    CASE
        WHEN descMoedaVaga IS NULL AND descPaisVaga = 'Internacional' THEN 'USD'
        ELSE descMoedaVaga
    END AS descMoedaVaga,

    vlSalario,
    descPronto,
    vlExpMinima,
    dtAtualizacao,
    dtCriacao

FROM tb_first