SELECT *,
       from_json(skillsId, 'array<string>') AS arraySkillsId

FROM {table}

QUALIFY row_number() OVER (PARTITION BY id ORDER BY updatedAt desc) = 1