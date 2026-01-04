{{ config(
    materialized = 'view',
    alias = 'silver_champindex_keyed'
) }}

SELECT
    s.account_name,
    s.owned_champion_id,
    l.champion_key,
    s.champion_id AS source_champion_id,
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
    s.snapshot_date,
    s.snapshot_version,
    s.schema_version
FROM {{ ref('stg_champindex__final') }} s
LEFT JOIN {{ ref('champion_id_map') }} l
  ON l.source_champion_id = s.champion_id
;