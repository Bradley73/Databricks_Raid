{{ config(severity = 'warn') }}

SELECT
    source_champion_id,
    COLLECT_SET(champion_name_norm) AS champion_names,
    COLLECT_SET(rarity)             AS rarities,
    COLLECT_SET(affinity)           AS affinities,
    COLLECT_SET(faction)            AS factions
FROM (
    SELECT
        source_champion_id,
        LOWER(TRIM(champion_name)) AS champion_name_norm,
        rarity,
        affinity,
        faction
    FROM {{ ref('stg_champindex__final') }}
)
GROUP BY source_champion_id
HAVING
    SIZE(COLLECT_SET(champion_name_norm)) > 1
    OR SIZE(COLLECT_SET(rarity)) > 1
    OR SIZE(COLLECT_SET(affinity)) > 1
    OR SIZE(COLLECT_SET(faction)) > 1
