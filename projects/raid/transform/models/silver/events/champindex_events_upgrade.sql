{{ config(
    materialized = 'incremental',
    alias = 'silver_champindex_events_upgrade',
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

changed_entities AS (
    SELECT DISTINCT
        s.account_name,
        s.owned_champion_id
    FROM {{ ref('champindex_scd2') }} AS s
    CROSS JOIN cutoff AS c
    WHERE s.valid_from > c.max_event_ts
),

candidate_rows AS (
    SELECT
        s.scd_id,
        s.account_name,
        s.owned_champion_id,
        s.champion_key,
        s.valid_from,
        s.rank,
        s.empower_level,
        s.blessing_grade,
        ROW_NUMBER() OVER (
            PARTITION BY s.account_name, s.owned_champion_id
            ORDER BY s.valid_from DESC, scd_id DESC
        ) AS rn_desc
    FROM {{ ref('champindex_scd2') }} AS s
    INNER JOIN changed_entities AS e
        ON  s.account_name = e.account_name
        AND s.owned_champion_id = e.owned_champion_id
),

incremental_scope AS (
    SELECT *
    FROM candidate_rows
    {% if is_incremental() %}
    WHERE rn_desc <= 2
    {% endif %}
),

with_prev AS (
    SELECT
        l.*,

        LAG(rank) OVER (
            PARTITION BY account_name, owned_champion_id
            ORDER BY valid_from, scd_id
        ) AS old_rank,

        LAG(empower_level) OVER (
            PARTITION BY account_name, owned_champion_id
            ORDER BY valid_from, scd_id
        ) AS old_empower_level,

        LAG(blessing_grade) OVER (
            PARTITION BY account_name, owned_champion_id
            ORDER BY valid_from, scd_id
        ) AS old_blessing_grade

    FROM incremental_scope AS l
),

events AS (
    SELECT
        CONCAT(wp.scd_id, '::', u.upgrade_type) AS event_id,
        wp.scd_id,
        wp.account_name,
        wp.owned_champion_id,
        wp.champion_key,
        wp.valid_from AS event_ts,
        u.upgrade_type,
        u.old_value,
        u.new_value,
        (u.new_value - u.old_value) AS delta_value
    FROM with_prev AS wp
    LATERAL VIEW STACK(
        3,
        'RANK',     wp.old_rank,           wp.rank,
        'EMPOWER',  wp.old_empower_level,  wp.empower_level,
        'BLESSING', wp.old_blessing_grade, wp.blessing_grade
    ) u AS upgrade_type, old_value, new_value
    WHERE u.old_value IS NOT NULL
      AND u.new_value > u.old_value
    {% if is_incremental() %}
      AND rn_desc = 1
    {% endif %}
)

SELECT
    --keys
    event_id,
    scd_id,
    account_name,
    owned_champion_id,
    champion_key,

    --event details
    event_ts,
    upgrade_type,
    old_value,
    new_value,
    delta_value
FROM events;
