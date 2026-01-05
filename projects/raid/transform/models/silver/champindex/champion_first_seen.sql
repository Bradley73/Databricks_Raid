{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['account_name', 'champion_key'],
    on_schema_change = 'sync_all_columns',
    alias = 'silver_champion_first_seen'
) }}

WITH watermark AS (

    {% if is_incremental() %}
    SELECT COALESCE(MAX(first_seen_snapshot_order_no), 0) AS max_order_no
    FROM {{ this }}
    {% else %}
    SELECT 0 AS max_order_no
    {% endif %}

),

new_snapshots AS (
    -- Only snapshots that are newer than what this feature spine has processed
    SELECT
        p.account_name,
        p.source_file,
        p.snapshot_order_no,
        p.snapshot_ts
    FROM {{ ref('ops_processed_snapshots') }} p
    CROSS JOIN watermark w
    WHERE p.is_out_of_order = FALSE
      AND p.snapshot_order_no > w.max_order_no
),

new_src AS (
    -- Only read champ rows for the new snapshot files
    SELECT
        s.account_name,
        s.champion_key,
        s.owned_champion_id,
        s.snapshot_ts,
        s.source_file,
        n.snapshot_order_no
    FROM {{ ref('champindex_keyed') }} s
    JOIN new_snapshots n
      ON n.account_name = s.account_name
     AND n.source_file  = s.source_file
),

candidates AS (

    {% if is_incremental() %}
    -- keep only unseen (account_name, champion_key)
    SELECT n.*
    FROM new_src n
    LEFT ANTI JOIN {{ this }} e
      ON e.account_name = n.account_name
     AND e.champion_key  = n.champion_key
    {% else %}
    SELECT *
    FROM new_src
    {% endif %}

),

one_row_per_key AS (
    SELECT
        account_name,
        champion_key,
        snapshot_ts            AS first_seen_ts,
        owned_champion_id      AS first_owned_champion_id,
        source_file            AS first_seen_source_file,
        snapshot_order_no      AS first_seen_snapshot_order_no,
        ROW_NUMBER() OVER (
            PARTITION BY account_name, champion_key
            ORDER BY snapshot_order_no ASC, snapshot_ts ASC
        ) AS rn
    FROM candidates
)

SELECT
    account_name,
    champion_key,
    first_seen_ts,
    first_owned_champion_id,
    first_seen_source_file,
    first_seen_snapshot_order_no
FROM one_row_per_key
WHERE rn = 1
;
