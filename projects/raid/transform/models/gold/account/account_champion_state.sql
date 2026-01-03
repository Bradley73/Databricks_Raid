{{ config(
    materialize = 'view'
    alias = 'gold_account_champion_state'
) }}

WITH base AS (
    SELECT DISTINCT
        account_name,
        champion_id
    FROM {{ ref("champindex_scd2") }}
),

agg AS (
    SELECT
        b.*,
        MAX(u.event_ts) AS last_upgraded_ts,
        MAX(scd.is_current) AS is_owned,
        SUM(scd.is_deleted) AS total_deleted,
        SUM(scd.is_current) AS duplicate_count,
        COUNT(o.*) AS total_seen,
        MAX(scd.empower_level) AS empower_level,
        MAX(scd.blessing_id) AS blessing_level,
        MAX(scd.rank) AS highest_rank,
        cfs.first_seen_ts,
        cl.champion_name,
        cl.rarity,
        cl.affinity,
        cl.faction
    FROM base
    INNER JOIN {{ ref("champion_lookup") }} cl
    INNER JOIN {{ ref('champindex_events_upgraded') }} u
        ON b.account_name = u.account_name
           AND b.champion_id = u.champion_id
    INNER JOIN {{ ref('champindex_events_obtained') }} o
        ON b.account_name = u.account_name
           AND b.champion_id = u.champion_id
    INNER JOIN {{ ref("champion_first_seen") }} cfs
        ON b.account_name = c.account_name
           AND b.champion_id = c.champion_id
    INNER JOIN {{ ref("champindex_scd2") }} scd
        ON b.account_name = scd.account_name
           AND b.champion_id = scd.champion_id
)

SELECT
    account_name,
    champion_name,
    rarity,
    affinity,
    faction,
    is_owned,
    first_seen_ts,
    last_upgraded_ts,
    total_seen,
    total_deleted,
    duplicate_count,
    highest_rank,
    empower_level,
    blessing_level
FROM agg
;
