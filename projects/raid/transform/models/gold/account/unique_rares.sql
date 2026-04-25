{{ config(
    materialized = 'view',
    alias = 'gold_unique_rares'
) }}

WITH rare_owned AS (

    SELECT
        account_name,
        champion_key,
        champion_name,
        rarity,
        affinity,
        faction,
        total_owned,
        total_deleted,
        total_obtained,
        max_rank,
        max_empower_level,
        max_blessing_level,
        first_seen_ts,
        last_upgraded_ts

    FROM {{ ref('account_champion_state') }}

    WHERE rarity = 3
      AND total_owned = 1

)

SELECT
    account_name,
    champion_key,
    champion_name,
    rarity,
    affinity,
    faction,

    total_owned,

    true AS is_protected_unique_rare,
    'KEEP_UNIQUE_RARE' AS recommendation,
    'ONLY_ONE_CURRENTLY_OWNED_RARE_COPY' AS decision_reason_code,

    total_deleted,
    total_obtained,
    max_rank,
    max_empower_level,
    max_blessing_level,
    first_seen_ts,
    last_upgraded_ts

FROM rare_owned
;