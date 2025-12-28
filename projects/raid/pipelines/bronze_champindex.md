# RAID Bronze Ingestion (Auto Loader + Ops Logging)

A production-minded bronze ingestion example built on **Databricks Auto Loader** (`cloudFiles`) with **RUN + FILE** operational logging. Sized for a small project / Databricks Free, but follows real-world ingestion + observability patterns.

---

## Features

- Incremental file ingestion with **Auto Loader** + **checkpointing** (avoids reprocessing)
- Explicit schema + `_rescued_data` capture for unknown columns
- Unity Catalog–safe source tracking via `_metadata.file_path`
- Bronze metadata enrichment (`run_id`, `snapshot_ts`, parsed `snapshot_date`, etc.)
- **RUN-level** outcome logging: `SUCCESS / EMPTY / FAILED`
- **FILE-level** success logging (one row per source file): row counts + rescued rows
- Best-effort logging that **never breaks ingestion** if the log write fails
- Clear separation between transformation logic and execution/logging logic

---

## What `availableNow` does

`trigger(availableNow=True)` makes the stream behave like a bounded run:

- processes **all currently available** new files,
- commits progress to the **checkpoint**,
- then **stops automatically** once caught up.

This is ideal for scheduled/on-demand ingestion runs where you want Auto Loader’s incremental discovery but don’t want a continuously running stream.

---

## How to run

1. Ensure your landing files exist under the configured landing path (e.g. `.../champindex/AccountName=<name>/champindex_<name>_<ddMMyyyy>_<n>.csv`).
2. Run the pipeline (notebook or job).
3. The run will:
   - ingest all available files to the bronze Delta table
   - write FILE-level success logs for processed files
   - write one RUN-level summary log row (`SUCCESS`, `EMPTY`, or `FAILED`)
   - stop automatically (`availableNow`)

---

## Logging schema

Logs are written to `ops.ingest_log` (append-only) with two event levels:

### RUN events (one row per pipeline run)
- `event_level = RUN`
- `status = SUCCESS | EMPTY | FAILED`
- `started_ts`, `finished_ts`
- `message`, `error_class` on failures
- includes stable metadata: `target_table_fqn`, `layer`, `pipeline_name`, `run_id`, `pipeline_run_ts`, etc.

### FILE events (one row per source file per micro-batch)
- `event_level = FILE`
- `status = SUCCESS`
- `source_file`
- `row_count`, `rows_rescued`
- `started_ts`, `finished_ts` represent the batch window for that file’s processing
- includes stable metadata: `target_table_fqn`, `layer`, `pipeline_name`, `run_id`, `pipeline_run_ts`, `batch_id`, etc.

---

## Limitations / scope

- Focuses on ingestion + logging, not orchestration (Jobs/ADF), alerting, or CI/CD.
- Designed for `availableNow` (batch-like) runs; continuous streaming would model RUN events differently.
- FILE failures are not emitted separately (RUN failure captures the error).
- Scaling/tuning for extremely high file counts and throughput is out of scope for this example.
