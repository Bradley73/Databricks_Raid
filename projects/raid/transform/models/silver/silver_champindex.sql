{{ config(
    materialized = 'table',
    alias = 'silver_champindex',
    post_hook = "{{ optimize_zorder(['owned_champion_id']) }}"
) }}

WITH files AS (
    SELECT DISTINCT
        account_name,
        source_file,
        snapshot_date,
        snapshot_version
    FROM {{ ref('stg_champindex') }}
),

latest_file AS (
    SELECT
        account_name,
        source_file,
        ROW_NUMBER() OVER (
            PARTITION BY account_name
            ORDER BY snapshot_date DESC, snapshot_version DESC
        ) AS rn
    FROM files
)

SELECT
    s.account_name,
    s.owned_champion_id,
    s.champion_id,
    s.champion_name,
    s.rank,
    s.level,
    s.empower_level,
    s.rarity,
    s.affinity,
    s.faction,

    s.used_t1_mastery_scrolls,
    s.unused_t1_mastery_scrolls,
    s.used_t2_mastery_scrolls,
    s.unused_t2_mastery_scrolls,
    s.used_t3_mastery_scrolls,
    s.unused_t3_mastery_scrolls,

    s.hp,
    s.atk,
    s.def,
    s.crit_rate,
    s.crit_damage,
    s.spd,
    s.acc,
    s.res,

    s.blessing_id,
    s.blessing_grade,

    s.run_id,
    s.source_file,
    s.snapshot_ts,
    s.snapshot_version,
    s.snapshot_date,
    s.schema_version

FROM {{ ref('stg_champindex') }} s
JOIN latest_file l
  ON s.source_file = l.source_file
WHERE l.rn = 1
;
