WITH recent_events AS (
    SELECT
        account_name,
        owned_champion_id,
        event_ts
    FROM {{ ref('champindex_events_obtained') }}
    WHERE event_ts >= (CURRENT_TIMESTAMP() - INTERVAL 30 DAYS)
),

scd2_min AS (
    SELECT
        s.account_name,
        s.owned_champion_id,
        MIN(s.valid_from) AS min_valid_from
    FROM {{ ref('champindex_scd2') }} s
    JOIN recent_events e
      ON s.account_name = e.account_name
     AND s.owned_champion_id = e.owned_champion_id
    GROUP BY s.account_name, s.owned_champion_id
)

SELECT
    e.account_name,
    e.owned_champion_id,
    e.event_ts,
    m.min_valid_from
FROM recent_events e
JOIN scd2_min m
  ON e.account_name = m.account_name
 AND e.owned_champion_id = m.owned_champion_id
WHERE e.event_ts <> m.min_valid_from
