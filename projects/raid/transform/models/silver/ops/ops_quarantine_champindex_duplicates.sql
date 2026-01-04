{{ config(
    materialized = 'view',
    alias = 'silver_ops_quarantine_champindex_duplicates'
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_champindex__dedupe_ranked') }}
    WHERE dup_count > 1
      AND rn > 1
)

SELECT
    account_name,
    owned_champion_id,
    champion_id,
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
    schema_version,

    'DUPLICATE_KEY:account_name+owned_champion_id+source_file' AS failure_reason,
    dup_count AS duplicate_group_count,
    rn AS duplicate_row_number

FROM src
;