{{ config(materialized='view', alias='silver_champindex_scd2') }}

WITH scd2_with_status AS (
    SELECT
        s.*,
        MAX(CASE WHEN s.dbt_valid_to IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY s.account_name, s.owned_champion_id) AS has_current
    FROM {{ ref('snap_champindex') }} s
)

SELECT
  --keys
  account_name,
  owned_champion_id,
  champion_key,
  source_champion_id,

  --change tracked data
  rank,
  level,
  empower_level,
  used_t1_mastery_scrolls,
  unused_t1_mastery_scrolls,
  used_t2_mastery_scrolls,
  unused_t2_mastery_scrolls,
  used_t3_mastery_scrolls,
  unused_t3_mastery_scrolls,
  hp,
  atk,
  def,
  crit_rate,
  crit_damage,
  spd,
  acc,
  res,
  blessing_id,
  blessing_grade,

  --scd2 cols
  dbt_valid_from AS valid_from,
  dbt_valid_to   AS valid_to,
  (dbt_valid_to IS NULL) AS is_current,
  (has_current = 0)      AS is_deleted,
  dbt_scd_id     AS scd_id

FROM scd2_with_status
;
