{{ config(
    materialized = 'view',
    alias = 'silver_ops_quarantine_champindex_current_keys'
) }}

WITH latest_file_per_account AS (
    SELECT
        account_name,
        source_file
    FROM (
        SELECT
            account_name,
            source_file,
            ROW_NUMBER() OVER (
                PARTITION BY account_name
                ORDER BY snapshot_order_no DESC
            ) AS rn
        FROM {{ ref('ops_processed_snapshots') }}
        WHERE is_out_of_order = FALSE
    ) d
    WHERE rn = 1
),

current_rows AS (
    SELECT
        s.*,
        CASE
            WHEN s.account_name IS NULL THEN 'NULL_KEY:account_name'
            WHEN s.owned_champion_id IS NULL THEN 'NULL_KEY:owned_champion_id'
            WHEN s.champion_id IS NULL THEN 'NULL_KEY:champion_id'
            WHEN s.source_file IS NULL THEN 'NULL_KEY:source_file'
        END AS failure_reason
    FROM {{ ref('stg_champindex') }} s
    JOIN latest_file_per_account l
      ON s.account_name = l.account_name
     AND s.source_file  = l.source_file
)

SELECT *
FROM current_rows
WHERE failure_reason IS NOT NULL
;
