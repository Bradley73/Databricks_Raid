{{ config(
    materialized = 'view',
    alias = 'silver_stg_champindex__classified'
) }}

WITH classified AS (
    SELECT
        s.*,

        /* -----------------------------
           QUARANTINE_REASON (first match wins)
           Order: keys → lineage → missing attrs → domain → range
        ----------------------------- */
        CASE
            /* Key nulls */
            WHEN s.account_name IS NULL THEN 'KEY_NULL:account_name'
            WHEN s.owned_champion_id IS NULL THEN 'KEY_NULL:owned_champion_id'
            WHEN s.champion_id IS NULL THEN 'KEY_NULL:champion_id'
            WHEN s.source_file IS NULL THEN 'KEY_NULL:source_file'

            /* Lineage / audit nulls */
            WHEN s.run_id IS NULL THEN 'LINEAGE_NULL:run_id'
            WHEN s.snapshot_ts IS NULL THEN 'LINEAGE_NULL:snapshot_ts'
            WHEN s.snapshot_date IS NULL THEN 'LINEAGE_NULL:snapshot_date'
            WHEN s.schema_version IS NULL THEN 'LINEAGE_NULL:schema_version'

            /* Missing attributes */
            WHEN s.champion_name IS NULL THEN 'ATTR_NULL:champion_name'

            /* Domain: accepted values */
            WHEN s.rank IS NULL OR s.rank NOT IN (1, 2, 3, 4, 5, 6) THEN 'DOMAIN:rank'
            WHEN s.empower_level IS NULL OR s.empower_level NOT IN (0, 1, 2, 3, 4) THEN 'DOMAIN:empower_level'
            WHEN s.rarity IS NULL OR s.rarity NOT IN (1, 2, 3, 4, 5, 6) THEN 'DOMAIN:rarity'
            WHEN s.affinity IS NULL OR s.affinity NOT IN (1, 2, 3, 4) THEN 'DOMAIN:affinity'
            WHEN s.blessing_id IS NULL OR s.blessing_id NOT IN (0, 1, 2, 3, 4, 5, 6) THEN 'DOMAIN:blessing_id'
            WHEN s.schema_version <> '1.0' THEN 'DOMAIN:schema_version'

            /* Range checks */
            WHEN s.faction IS NULL OR s.faction < 1 OR s.faction > 18 THEN 'RANGE:faction'
            WHEN s.level IS NULL OR s.level < 0 THEN 'RANGE:level'
            WHEN s.used_t1_mastery_scrolls IS NULL OR s.used_t1_mastery_scrolls < 0 THEN 'RANGE:used_t1_mastery_scrolls'
            WHEN s.unused_t1_mastery_scrolls IS NULL OR s.unused_t1_mastery_scrolls < 0 THEN 'RANGE:unused_t1_mastery_scrolls'
            WHEN s.used_t2_mastery_scrolls IS NULL OR s.used_t2_mastery_scrolls < 0 THEN 'RANGE:used_t2_mastery_scrolls'
            WHEN s.unused_t2_mastery_scrolls IS NULL OR s.unused_t2_mastery_scrolls < 0 THEN 'RANGE:unused_t2_mastery_scrolls'
            WHEN s.used_t3_mastery_scrolls IS NULL OR s.used_t3_mastery_scrolls < 0 THEN 'RANGE:used_t3_mastery_scrolls'
            WHEN s.unused_t3_mastery_scrolls IS NULL OR s.unused_t3_mastery_scrolls < 0 THEN 'RANGE:unused_t3_mastery_scrolls'
            WHEN s.hp IS NULL OR s.hp < 0 THEN 'RANGE:hp'
            WHEN s.atk IS NULL OR s.atk < 0 THEN 'RANGE:atk'
            WHEN s.def IS NULL OR s.def < 0 THEN 'RANGE:def'
            WHEN s.crit_rate IS NULL OR s.crit_rate < 0 THEN 'RANGE:crit_rate'
            WHEN s.crit_damage IS NULL OR s.crit_damage < 0 THEN 'RANGE:crit_damage'
            WHEN s.spd IS NULL OR s.spd < 0 THEN 'RANGE:spd'
            WHEN s.acc IS NULL OR s.acc < 0 THEN 'RANGE:acc'
            WHEN s.res IS NULL OR s.res < 0 THEN 'RANGE:res'
            WHEN s.blessing_grade IS NULL OR s.blessing_grade < 0 THEN 'RANGE:blessing_grade'
            WHEN s.snapshot_version IS NULL OR s.snapshot_version < 0 THEN 'RANGE:snapshot_version'

            ELSE NULL
        END AS quarantine_reason,

        /* -----------------------------
           QUARANTINE_BUCKET
        ----------------------------- */
        CASE
            WHEN s.account_name IS NULL
              OR s.owned_champion_id IS NULL
              OR s.champion_id IS NULL
              OR s.source_file IS NULL
              THEN 'KEY'

            WHEN s.run_id IS NULL
              OR s.snapshot_ts IS NULL
              OR s.snapshot_date IS NULL
              OR s.schema_version IS NULL
              THEN 'LINEAGE'

            WHEN s.champion_name IS NULL
              THEN 'MISSING_ATTR'

            WHEN (s.rank IS NULL OR s.rank NOT IN (1, 2, 3, 4, 5, 6))
              OR (s.empower_level IS NULL OR s.empower_level NOT IN (0, 1, 2, 3, 4))
              OR (s.rarity IS NULL OR s.rarity NOT IN (1, 2, 3, 4, 5, 6))
              OR (s.affinity IS NULL OR s.affinity NOT IN (1, 2, 3, 4))
              OR (s.blessing_id IS NULL OR s.blessing_id NOT IN (0, 1, 2, 3, 4, 5, 6))
              OR (s.schema_version IS NULL OR s.schema_version <> '1.0')
              THEN 'DOMAIN'

            WHEN (s.faction IS NULL OR s.faction < 1 OR s.faction > 18)
              OR (s.level IS NULL OR s.level < 0)
              OR (s.used_t1_mastery_scrolls IS NULL OR s.used_t1_mastery_scrolls < 0)
              OR (s.unused_t1_mastery_scrolls IS NULL OR s.unused_t1_mastery_scrolls < 0)
              OR (s.used_t2_mastery_scrolls IS NULL OR s.used_t2_mastery_scrolls < 0)
              OR (s.unused_t2_mastery_scrolls IS NULL OR s.unused_t2_mastery_scrolls < 0)
              OR (s.used_t3_mastery_scrolls IS NULL OR s.used_t3_mastery_scrolls < 0)
              OR (s.unused_t3_mastery_scrolls IS NULL OR s.unused_t3_mastery_scrolls < 0)
              OR (s.hp IS NULL OR s.hp < 0)
              OR (s.atk IS NULL OR s.atk < 0)
              OR (s.def IS NULL OR s.def < 0)
              OR (s.crit_rate IS NULL OR s.crit_rate < 0)
              OR (s.crit_damage IS NULL OR s.crit_damage < 0)
              OR (s.spd IS NULL OR s.spd < 0)
              OR (s.acc IS NULL OR s.acc < 0)
              OR (s.res IS NULL OR s.res < 0)
              OR (s.blessing_grade IS NULL OR s.blessing_grade < 0)
              OR (s.snapshot_version IS NULL OR s.snapshot_version < 0)
              THEN 'RANGE'

            ELSE NULL
        END AS quarantine_bucket

    FROM {{ ref('stg_champindex') }} s
),

flags AS (
    SELECT
        c.*,
        CASE WHEN c.quarantine_reason IS NOT NULL THEN TRUE ELSE FALSE END AS is_quarantined,
        CASE WHEN c.quarantine_bucket = 'KEY' THEN TRUE ELSE FALSE END AS is_quarantined_key,
        CASE WHEN c.quarantine_bucket IN ('DOMAIN', 'RANGE', 'MISSING_ATTR') THEN TRUE ELSE FALSE END AS is_quarantined_data,
        CASE WHEN c.quarantine_bucket = 'LINEAGE' THEN TRUE ELSE FALSE END AS is_quarantined_lineage

    FROM classified c
)

SELECT
    /* Base stg_champindex contract */
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

    /* Classification contract */
    quarantine_reason,
    quarantine_bucket,
    is_quarantined,
    is_quarantined_key,
    is_quarantined_data,
    is_quarantined_lineage

FROM flags;