{{ config(
    materialized = 'view',
    alias = 'silver_ops_out_of_order_snapshots'
) }}

SELECT
    account_name,
    source_file,
    snapshot_ts,
    snapshot_date,
    snapshot_version,
    snapshot_order_no,
    event_sort_key
FROM {{ ref('ops_processed_snapshots') }}
WHERE is_out_of_order = TRUE
;
