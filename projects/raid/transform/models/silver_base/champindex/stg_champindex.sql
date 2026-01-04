{{ config(
    materialized = 'view',
    alias = 'silver_stg_champindex'
) }}

SELECT
    -- Business keys
    AccountName              AS account_name,
    ID                       AS owned_champion_id,

    -- Champion identity (not unique to owned copy)
    HeroID                   AS source_champion_id,

    -- Descriptors
    Name                     AS champion_name,
    Rank                     AS rank,
    Level                    AS level,
    EmpowerLevel             AS empower_level,
    Rarity                   AS rarity,
    Affinity                 AS affinity,
    Faction                  AS faction,

    -- Scroll usage
    UsedT1MasScrolls         AS used_t1_mastery_scrolls,
    UnUsedT1MasScrolls       AS unused_t1_mastery_scrolls,
    UsedT2MasScrolls         AS used_t2_mastery_scrolls,
    UnUsedT2MasScrolls       AS unused_t2_mastery_scrolls,
    UsedT3MasScrolls         AS used_t3_mastery_scrolls,
    UnUsedT3MasScrolls       AS unused_t3_mastery_scrolls,

    -- Stats
    HP                       AS hp,
    ATK                      AS atk,
    DEF                      AS def,
    CritRate                 AS crit_rate,
    CritDamage               AS crit_damage,
    SPD                      AS spd,
    ACC                      AS acc,
    RES                      AS res,

    -- Blessing
    BlessingID               AS blessing_id,
    BlessingGrade            AS blessing_grade,

    -- Lineage / audit (keep)
    run_id                   AS run_id,
    source_file              AS source_file,
    snapshot_ts              AS snapshot_ts,
    snapshot_date            AS snapshot_date,
    snapshot_version         AS snapshot_version,
    schema_version           AS schema_version

FROM {{ source('bronze', 'bronze_champindex') }}
;
