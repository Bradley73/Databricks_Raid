{{ config(
    materialized = 'incremental',
    alias = 'silver_champion_lookup',
    incremental_strategy = 'merge',
    unique_key = ['champion_id'],
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT DISTINCT
        champion_id,
        champion_name,
        rarity,
        affinity,
        faction
    FROM {{ ref('stg_champindex') }}
),

candidates AS (
    {% if is_incremental() %}
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN {{ this }} t
      ON b.champion_id = t.champion_id
    {% else %}
    SELECT *
    FROM base
    {% endif %}
)

SELECT
    champion_id,
    champion_name,
    rarity,
    affinity,
    faction
FROM candidates
;
