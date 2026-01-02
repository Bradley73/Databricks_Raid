{{ config(
    materialized = 'incremental',
    alias = 'silver_champindex_events_obtained',
    incremental_strategy = 'merge',
    unique_key = 'event_id'
) }}

WITH cutoff AS (
    SELECT
        {% if is_incremental() %}
        COALESCE((SELECT MAX(event_ts) FROM {{ this }}), TIMESTAMP('1900-01-01'))
        {% else %}
        TIMESTAMP('1900-01-01')
        {% endif %}
        AS max_event_ts
),

base AS (
    SELECT
        scd_id,
        account_name,
        owned_champion_id,
        champion_id,
        valid_from
    FROM {{ ref('silver_champindex_scd2') }} AS s
    CROSS JOIN cutoff AS c
    WHERE s.valid_from > c.max_event_ts
),

obtained AS (
    SELECT
        b.*
    FROM base AS b
    LEFT ANTI JOIN {{ ref('silver_champindex_scd2') }} AS s0
        ON  s0.account_name = b.account_name
        AND s0.owned_champion_id = b.owned_champion_id
        AND (
            s0.valid_from < b.valid_from
            OR (s0.valid_from = b.valid_from AND s0.scd_id < b.scd_id)
        )
)

SELECT
    scd_id AS event_id,
    scd_id,
    account_name,
    owned_champion_id,
    champion_id,
    valid_from AS event_ts
FROM obtained;
