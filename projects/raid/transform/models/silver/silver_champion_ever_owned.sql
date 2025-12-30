{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['account_name', 'champion_id'],
    on_schema_change = 'sync_all_columns'
) }}

WITH new_src AS (

    SELECT
        account_name,
        champion_id,
        owned_champion_id,
        snapshot_ts,
        source_file
    FROM {{ ref('stg_champindex') }}

    {% if is_incremental() %}
      WHERE snapshot_ts > (
          SELECT COALESCE(MAX(first_seen_ts), CAST('1900-01-01' AS TIMESTAMP))
          FROM {{ this }}
      )
    {% endif %}

),

candidates AS (

    {% if is_incremental() %}
    -- Keep only champs not already in ever_owned
    SELECT n.*
    FROM new_src n
    LEFT ANTI JOIN /*+ BROADCAST(e) */ {{ this }} e
        ON e.account_name = n.account_name
       AND e.champion_id  = n.champion_id
    {% else %}
    -- First build: everything is a candidate
    SELECT *
    FROM new_src
    {% endif %}

),

one_row_per_key AS (

    SELECT
        account_name,
        champion_id,
        snapshot_ts AS first_seen_ts,
        owned_champion_id AS first_owned_champion_id,
        source_file AS first_seen_source_file,
        ROW_NUMBER() OVER (
            PARTITION BY account_name, champion_id
            ORDER BY snapshot_ts ASC
        ) AS rn
    FROM candidates

)

SELECT
    account_name,
    champion_id,
    first_seen_ts,
    first_owned_champion_id,
    first_seen_source_file
FROM one_row_per_key
WHERE rn = 1
;