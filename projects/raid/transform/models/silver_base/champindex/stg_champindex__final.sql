{{ config(
    materialized = 'view',
    alias = 'silver_stg_champindex__final'
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_champindex__dedupe_ranked') }}
    WHERE rn = 1
)

SELECT
    account_name,
    owned_champion_id,
    source_champion_id,
    champion_name,
    rank,
    level,
    empower_level,
    rarity,
    affinity,
    faction,
    used_t1_mastery_scrolls,
    unused_t1_mastery_scrolls,
    used_t2_mastery_scrolls,
    unused_t2_mastery_scrolls,
    used_t3_mastery_scrolls,
    unused_t3_mastery_scrolls,
    hp,
    atk,
    def,
    crit_rate,
    crit_damage,
    spd,
    acc,
    res,
    blessing_id,
    blessing_grade,
    run_id,
    source_file,
    snapshot_ts,
    snapshot_date,
    snapshot_version,
    schema_version
FROM src
;