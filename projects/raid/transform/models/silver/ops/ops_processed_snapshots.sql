{{ config(
    materialized = 'incremental',
    alias = 'silver_ops_processed_snapshots',
    incremental_strategy = 'merge',
    unique_key = ['account_name', 'source_file'],
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT DISTINCT
        account_name,
        source_file,
        snapshot_ts,
        snapshot_date,
        snapshot_version
    FROM {{ ref('champindex_keyed') }}
),

accounts_in_batch AS (
    SELECT DISTINCT account_name
    FROM base
),

latest_existing AS (

    {% if is_incremental() %}

    SELECT
        t.account_name,
        MAX(t.snapshot_order_no) AS max_order_no,
        MAX(t.snapshot_ts)       AS max_ts,
        MAX(t.event_sort_key)    AS max_sort_key
    FROM {{ this }} t
    JOIN accounts_in_batch a
      ON a.account_name = t.account_name
    GROUP BY t.account_name

    {% else %}

    -- First run: no existing state yet
    SELECT
        a.account_name,
        0                           AS max_order_no,
        CAST('1900-01-01' AS TIMESTAMP) AS max_ts,
        '00000000-0000-'            AS max_sort_key
    FROM accounts_in_batch a

    {% endif %}
),

new_rows AS (
    SELECT
        b.account_name,
        b.source_file,
        b.snapshot_ts,
        b.snapshot_date,
        b.snapshot_version,
        l.max_order_no,
        l.max_ts,
        l.max_sort_key
    FROM base b
    JOIN latest_existing l
      ON l.account_name = b.account_name
    WHERE b.snapshot_ts > l.max_ts
),

final AS (
    SELECT
        account_name,

        max_order_no + ROW_NUMBER() OVER (
            PARTITION BY account_name
            ORDER BY snapshot_date, snapshot_version, source_file
        ) AS snapshot_order_no,

        CONCAT(
            date_format(snapshot_date, 'yyyyMMdd'),
            '-',
            lpad(CAST(snapshot_version AS STRING), 4, '0'),
            '-',
            source_file
        ) AS event_sort_key,

        max_sort_key,
        source_file,
        snapshot_ts,
        snapshot_date,
        snapshot_version
    FROM new_rows
)

SELECT
    account_name,
    snapshot_order_no,
    event_sort_key,
    (event_sort_key < max_sort_key) AS is_out_of_order,
    source_file,
    snapshot_ts,
    snapshot_date,
    snapshot_version
FROM final
;
