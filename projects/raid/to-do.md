To-do list (tick-off friendly)
A) Foundations

 Create/confirm int_champindex_snapshot_deduped (one row per account + owned_champion_id + snapshot_ts; clean types)

 Create silver_champindex_current (latest snapshot per account/owned_champion_id)

B) State table

 Build silver_champion_ever_owned incremental (from staged snapshots or from SCD2 later)

 Decide watermark strategy (dbt incremental predicate + unique key)

C) History backbone

 Build silver_champindex_scd2_full (incremental merge, hash-based change detection)

 Add delete detection + end-date records

 Build silver_champindex_scd2_active_history (prune by owned_champion_id)

D) Events

 Build silver_champindex_events_upgrade (rank/empower_level/blessing_grade increases; include old/new)

 (Later) Build silver_champion_events_obtained (best-effort “reappeared” inference; used for empowerment candidates)

E) Later hardening

 Add tests/contracts + rejected rows quarantine (your next step after experiments)