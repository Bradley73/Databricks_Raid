{{ config(
    materialized = 'view',
    alias = 'silver_champindex_current'
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
        -- keys
        s.account_name,
        s.owned_champion_id,
        s.champion_id,

        -- tracked fields
        s.rank,
        s.level,
        s.empower_level,

        s.used_t1_mastery_scrolls,
        s.unused_t1_mastery_scrolls,
        s.used_t2_mastery_scrolls,
        s.unused_t2_mastery_scrolls,
        s.used_t3_mastery_scrolls,
        s.unused_t3_mastery_scrolls,

        s.hp,
        s.atk,
        s.def,
        s.crit_rate,
        s.crit_damage,
        s.spd,
        s.acc,
        s.res,

        s.blessing_id,
        s.blessing_grade,

        -- lineage
        s.source_file
    FROM {{ ref('stg_champindex__final') }} s
    JOIN latest_file_per_account l
      ON s.account_name = l.account_name
     AND s.source_file  = l.source_file
)

SELECT *
FROM current_rows
WHERE account_name IS NOT NULL
  AND owned_champion_id IS NOT NULL
  AND champion_id IS NOT NULL
  AND source_file IS NOT NULL
;
