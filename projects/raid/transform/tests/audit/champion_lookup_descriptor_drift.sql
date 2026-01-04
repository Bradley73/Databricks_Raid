-- Flags champion_ids whose descriptors drift in stg_champindex (name/rarity/affinity/faction).
-- This is a "dimension stability" audit; consider running on schedule or as WARN severity.

WITH combos AS (
    SELECT
        champion_id,
        count(DISTINCT concat_ws('||',
            cast(champion_name AS string),
            cast(rarity AS string),
            cast(affinity AS string),
            cast(faction AS string)
        )) AS distinct_descriptor_sets
    FROM {{ ref('stg_champindex') }}
    WHERE champion_id IS NOT NULL
    GROUP BY champion_id
)
SELECT *
FROM combos
WHERE distinct_descriptor_sets > 1
