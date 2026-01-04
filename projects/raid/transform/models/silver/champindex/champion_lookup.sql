{{ config(
    materialized = 'incremental',
    alias = 'silver_champion_lookup',
    incremental_strategy = 'merge',
    unique_key = ['champion_key'],
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT DISTINCT
        champion_key,
        champion_name,
        rarity,
        affinity,
        faction
    FROM {{ ref('champindex_keyed') }}
),

candidates AS (
    {% if is_incremental() %}
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN {{ this }} t
      ON b.champion_key = t.champion_key
    {% else %}
    SELECT *
    FROM base
    {% endif %}
)

SELECT
    champion_key,
    champion_name,
    rarity,
    affinity,
    faction
FROM candidates
;
