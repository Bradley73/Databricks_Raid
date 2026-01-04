{{ config(materialized = 'ephemeral') }}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_champindex__classified') }}
    WHERE is_quarantined = FALSE
),

ranked AS (
    SELECT
        src.*,
        ROW_NUMBER() OVER (
            PARTITION BY account_name, owned_champion_id, source_file
            ORDER BY snapshot_version DESC, snapshot_ts DESC, run_id DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY account_name, owned_champion_id, source_file
        ) AS dup_count
    FROM src
)

SELECT *
FROM ranked;
