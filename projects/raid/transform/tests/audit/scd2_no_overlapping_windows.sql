WITH ordered AS (
    SELECT
        account_name,
        owned_champion_id,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY account_name, owned_champion_id
            ORDER BY valid_from, scd_id
        ) AS next_valid_from
    FROM {{ ref('champindex_scd2') }}
),

violations AS (
    SELECT
        account_name,
        owned_champion_id,
        valid_from,
        valid_to,
        next_valid_from
    FROM ordered
    WHERE valid_to IS NOT NULL
      AND next_valid_from IS NOT NULL
      AND valid_to > next_valid_from
)

SELECT *
FROM violations
;
