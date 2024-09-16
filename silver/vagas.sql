SELECT id AS idVaga,
       topicId AS idTopico,
       url AS urlVaga,
       description AS descVaga,
       title AS descTituloVaga,
       company AS descEmpresa,
       language AS descLinguagemVaga,
       country AS descPaisVaga,
       currency AS descMoedaVaga,
       salary AS vlSalario,
       ready AS descPronto,
       minimumYears AS vlExpMinima,
       updatedAt AS dtAtualizacao,
       createdAt AS dtCriacao

FROM bronze.trampar_de_casa.roles