{{ config(
    materialized = 'view',
    alias = 'gold_account_champion_state'
) }}

WITH base AS (
    SELECT account_name, champion_id,
        SUM(is_current) AS total_owned,
        SUM(is_deleted) AS total_deleted,
        MAX(empower_level) AS max_empower_level,
        MAX(blessing_id) AS max_blessing_level,
        MAX(rank) AS max_rank
    FROM {{ ref("champindex_scd2") }}
    GROUP BY account_name, champion_id
),

events_obtained AS (
    SELECT account_name, champion_id,
        COUNT(*) AS total_obtained
    FROM {{ ref('champindex_events_obtained') }}
    GROUP BY account_name, champion_id
),

events_upgraded AS (
    SELECT account_name, champion_id,
        MAX(event_ts) AS last_upgraded_ts
    FROM {{ ref('champindex_events_upgrade') }}
    GROUP BY account_name, champion_id
),

final AS (
    SELECT
        b.*,
        o.total_obtained,
        u.last_upgraded_ts,
        cfs.first_seen_ts,
        cl.champion_name,
        cl.rarity,
        cl.affinity,
        cl.faction
    FROM base b
    LEFT JOIN events_obtained o
        ON b.account_name = o.account_name
           AND b.champion_id = o.champion_id
    LEFT JOIN events_upgraded u
        ON b.account_name = u.account_name
           AND b.champion_id = u.champion_id
    JOIN {{ ref("champion_first_seen") }} cfs
        ON b.account_name = cfs.account_name
           AND b.champion_id = cfs.champion_id
    JOIN {{ ref("champion_lookup") }} cl
        ON b.champion_id = cl.champion_id
)

SELECT
    account_name,
    champion_name,
    rarity,
    affinity,
    faction,
    first_seen_ts,
    last_upgraded_ts,
    total_owned,
    total_deleted,
    total_obtained,
    max_rank,
    max_empower_level,
    max_blessing_level
FROM final
;
