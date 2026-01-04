{{ config(
    materialized = 'view',
    alias = 'silver_ops_last_run_summary'
) }}

WITH latest_snapshot_per_account AS (
    SELECT
        account_name,
        source_file,
        snapshot_ts,
        snapshot_date,
        snapshot_version,
        snapshot_order_no,
        is_out_of_order,
        ROW_NUMBER() OVER (
            PARTITION BY account_name
            ORDER BY snapshot_order_no DESC
        ) AS rn
    FROM {{ ref('ops_processed_snapshots') }}
),

latest AS (
    SELECT *
    FROM latest_snapshot_per_account
    WHERE rn = 1
),

champ_rows AS (
    SELECT
        s.account_name,
        s.source_file,
        COUNT(*) AS champ_rows_in_snapshot
    FROM {{ ref('champindex_keyed') }} s
    JOIN latest l
      ON s.account_name = l.account_name
     AND s.source_file  = l.source_file
    GROUP BY s.account_name, s.source_file
),

quarantine_rows AS (
    SELECT
        q.account_name,
        q.source_file,
        COUNT(*) AS quarantine_key_rows_in_snapshot
    FROM {{ ref('ops_quarantine_champindex_classified') }} q
    JOIN latest l
      ON q.account_name = l.account_name
     AND q.source_file  = l.source_file
    GROUP BY q.account_name, q.source_file
)

SELECT
    l.account_name,
    l.source_file,
    l.snapshot_ts,
    l.snapshot_date,
    l.snapshot_version,
    l.snapshot_order_no,
    l.is_out_of_order,

    COALESCE(c.champ_rows_in_snapshot, 0) AS champ_rows_in_snapshot,
    COALESCE(q.quarantine_key_rows_in_snapshot, 0) AS quarantine_key_rows_in_snapshot

FROM latest l
LEFT JOIN champ_rows c
  ON c.account_name = l.account_name
 AND c.source_file  = l.source_file
LEFT JOIN quarantine_rows q
  ON q.account_name = l.account_name
 AND q.source_file  = l.source_file
;
