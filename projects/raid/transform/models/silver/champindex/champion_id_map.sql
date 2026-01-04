{{ config(
    materialized = 'incremental',
    alias = 'silver_champion_id_map',
    incremental_strategy = 'merge',
    unique_key = ['source_champion_id'],
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT DISTINCT
        --Old ID with all champion dimensional attributes
        source_champion_id,
        champion_name,
        rarity,
        affinity,
        faction,
        ROW_NUMBER() OVER (
            PARTITION BY source_champion_id
            ORDER BY
                -- pick a stable winner deterministically
                lower(trim(champion_name)) ASC,
                rarity ASC,
                affinity ASC,
                faction ASC
        ) AS rn
    FROM {{ ref('stg_champindex__final') }}
),

candidates AS (
    {% if is_incremental() %}
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN {{ this }} t
      ON b.source_champion_id = t.source_champion_id
    {% else %}
    SELECT *
    FROM base
    {% endif %}
)

SELECT
    sha2(
        concat_ws(
            '||',
            lower(trim(champion_name)),
            cast(rarity   as string),
            cast(affinity as string),
            cast(faction  as string)
        ),
        256
    ) AS champion_key,
    CAST(source_champion_id / 10 AS INT) AS inferred_champion_number,
    source_champion_id
FROM candidates
WHERE rn = 1
;
