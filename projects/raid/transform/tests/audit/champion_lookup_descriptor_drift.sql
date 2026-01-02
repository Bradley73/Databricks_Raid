-- Flags champion_ids whose descriptors drift in stg_champindex (name/rarity/affinity/faction).
-- This is a "dimension stability" audit; consider running on schedule or as WARN severity.

with combos as (
    select
        champion_id,
        count(distinct concat_ws('||',
            cast(champion_name as string),
            cast(rarity as string),
            cast(affinity as string),
            cast(faction as string)
        )) as distinct_descriptor_sets
    from {{ ref('stg_champindex') }}
    where champion_id is not null
    group by champion_id
)
select *
from combos
where distinct_descriptor_sets > 1
