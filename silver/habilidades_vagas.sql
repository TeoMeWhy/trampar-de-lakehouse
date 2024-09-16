SELECT id AS idVaga,
       int(skills) AS idHabilidade

FROM bronze.trampar_de_casa.roles

LATERAL VIEW explode(arraySkillsId) AS skills