SELECT id AS idVaga,
       explode(arraySkillsId) AS idHabilidade

FROM bronze.trampar_de_casa.roles