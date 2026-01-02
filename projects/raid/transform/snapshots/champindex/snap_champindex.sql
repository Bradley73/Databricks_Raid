{% snapshot snap_champindex %}

-- strategy: no reliable updated_at for the source system, so use check strategy
-- check_cols: only these fields trigger a new SCD2 version (your list)
-- invalidate_hard_deleted: if a champion disappears from "current", close out the record
{{
  config(
    target_database = 'workspace',
    target_schema   = 'raid',

    unique_key = "concat(account_name, '::', owned_champion_id)",

    strategy = 'check',

    check_cols = [
      'champion_id',
      'rank',
      'level',
      'empower_level',
      'used_t1_mastery_scrolls',
      'unused_t1_mastery_scrolls',
      'used_t2_mastery_scrolls',
      'unused_t2_mastery_scrolls',
      'used_t3_mastery_scrolls',
      'unused_t3_mastery_scrolls',
      'hp',
      'atk',
      'def',
      'crit_rate',
      'crit_damage',
      'spd',
      'acc',
      'res',
      'blessing_id',
      'blessing_grade'
    ],

    invalidate_hard_deletes = true
  )
}}

SELECT
    --keys
    account_name,
    owned_champion_id,
    champion_id,

    --tracked fields
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

    --lineage
    source_file
FROM {{ ref('champindex_current') }}

{% endsnapshot %}
