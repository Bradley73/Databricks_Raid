{{ config(
    materialized = 'view',
    alias = 'silver_ops_quarantine_champindex_classified'
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_champindex__classified') }}
    WHERE is_quarantined = TRUE
)

SELECT
    /* Base stg_champindex contract */
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
    schema_version,

    /* Quarantine classification */
    quarantine_reason,
    quarantine_bucket,
    is_quarantined,
    is_quarantined_key,
    is_quarantined_data,
    is_quarantined_lineage,

    /* Ops-friendly naming (optional but handy) */
    quarantine_reason AS failure_reason

FROM src
;