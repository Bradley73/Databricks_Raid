#!/usr/bin/env python3
"""RAID Bronze Ingestion: champindex

Executable PySpark pipeline designed for Databricks (Auto Loader + ops logging).

- Reads CSV files incrementally using Auto Loader (cloudFiles)
- Writes to a bronze Delta table
- Emits FILE-level SUCCESS log rows per source_file
- Emits one RUN-level log row per execution: SUCCESS / EMPTY / FAILED

Notes:
- Assumes Databricks environment (Unity Catalog / Volumes paths, Auto Loader).
- Uses best-effort logging: logging failures never break ingestion.
"""

from __future__ import annotations

# pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# stdlib
from datetime import datetime, timezone
import uuid

# raid (project-specific)
from projects.raid.schema.champindex.schema import (
    DATA_SCHEMA,
    BRONZE_COLS,
    SCHEMA_VERSION,
)

# shared logging / ops
from shared.logging.ingest_logging import write_log_best_effort
from shared.logging.ingest_log_constants import IngestStatus
from shared.logging.ingest_log_context import LogContext
from shared.logging.ingest_log_builders import (
    run_event,
    file_success_events,
)


# -----------------------
# Config (edit as needed)
# -----------------------
TARGET_TABLE_FQN = "raid.bronze_champindex"
PIPELINE_NAME = "01_ingest_bronze"
LAYER = "bronze"

LANDING_BASE = "/Volumes/workspace/raid/champindex"
SCHEMA_LOCATION = "/Volumes/workspace/raid/_system/autoloader_schemas/champindex_bronze"
CHECKPOINT_LOCATION = "/Volumes/workspace/raid/_system/checkpoints/01_ingest_bronze/champindex"

CSV_OPTS = {
    "header": "true",
    "sep": ";",
    "quote": '"',
    "escape": '"',
    "mode": "PERMISSIVE",
}


def build_log_context(*, run_id: str) -> LogContext:
    """Build stable pipeline-wide log context."""
    return LogContext(
        target_table_fqn=TARGET_TABLE_FQN,
        layer=LAYER,
        pipeline_name=PIPELINE_NAME,
        run_id=run_id,
        checkpoint_location=CHECKPOINT_LOCATION,
        schema_location=SCHEMA_LOCATION,
        base_context={"trigger": "availableNow"},
    )


def build_bronze_df(spark: SparkSession, *, run_id: str) -> DataFrame:
    """Read with Auto Loader and apply bronze shaping once."""
    source_file_col = F.col("_metadata.file_path")
    file_name_col = F.element_at(F.split(source_file_col, "/"), -1)

    account_name_col = F.regexp_extract(source_file_col, r"/AccountName=([^/]+)/", 1)

    ddmmyyyy_col = F.regexp_extract(file_name_col, r"^champindex_[^_]+_(\d{8})_\d+\.csv$", 1)
    snapshot_date_col = F.try_to_date(ddmmyyyy_col, "ddMMyyyy")
    snapshot_version_col = F.expr(
        "try_cast(regexp_extract(source_file, '^champindex_[^_]+_\\d{8}_(\\d+)\\.csv$', 1) AS INT)"
    )
    
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("ignoreMissingFiles", "true")
        .options(**CSV_OPTS)
        .schema(DATA_SCHEMA)
        .load(LANDING_BASE)
    )

    return (
        raw_stream.withColumn("source_file", source_file_col)
        .withColumn("AccountName", account_name_col)
        .withColumn("snapshot_ts", F.current_timestamp())
        .withColumn("snapshot_date", snapshot_date_col)
        .withColumn("snapshot_version", snapshot_version_col)
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
        .withColumn("run_id", F.lit(run_id))  # IMPORTANT: before select(*BRONZE_COLS)
        .select(*BRONZE_COLS)
    )


def run_pipeline(spark: SparkSession) -> None:
    # Run identity
    run_id = str(uuid.uuid4())

    # Static timestamp of run start (Python), converted once to Spark column literal
    pipeline_run_ts_py = datetime.now(timezone.utc)
    pipeline_run_ts_col = F.lit(pipeline_run_ts_py).cast("timestamp")

    # Context shared by all events for this run
    log_ctx = build_log_context(run_id=run_id)

    # Build bronze streaming DF
    bronze_df = build_bronze_df(spark, run_id=run_id)

    # =====================================================================
    # END OF INGESTION / TRANSFORMATION LOGIC
    # ---------------------------------------------------------------------
    # Below this point:
    #   - No schema or parsing changes should be made
    #   - DataFrames are considered FINAL for writing
    #   - Only execution concerns are handled:
    #       * writing to bronze
    #       * ingestion logging
    #       * error handling
    # =====================================================================

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        started_ts = datetime.now(timezone.utc)

        # NOTE: empty micro-batches are ignored; RUN status is derived from recentProgress
        if not batch_df.limit(1).collect():
            return

        # Write bronze
        (
            batch_df.write.format("delta")
            .mode("append")
            .saveAsTable(TARGET_TABLE_FQN)
        )

        finished_ts = datetime.now(timezone.utc)

        # FILE SUCCESS rows (per file in this batch)
        write_log_best_effort(
            file_success_events(
                batch_df,
                log_ctx,
                pipeline_run_ts_col=pipeline_run_ts_col,
                batch_id=int(batch_id),
                started_ts_col=F.lit(started_ts).cast("timestamp"),
                finished_ts_col=F.lit(finished_ts).cast("timestamp"),
                rescued_col="_rescued_data",
            )
        )

    # Start stream (bounded run)
    query = (
        bronze_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )

    # RUN END (once) - SUCCESS / EMPTY / FAILED
    try:
        query.awaitTermination()

        # Determine EMPTY vs SUCCESS from streaming progress
        total_in = 0
        for p in query.recentProgress:
            try:
                total_in += int(p.get("numInputRows", 0))
            except Exception:
                pass

        end_status = IngestStatus.EMPTY if total_in == 0 else IngestStatus.SUCCESS
        end_msg = "No new data files found" if total_in == 0 else "Stream completed successfully"

        write_log_best_effort(
            run_event(
                spark,
                log_ctx,
                pipeline_run_ts_col=pipeline_run_ts_col,
                batch_id=None,
                status=end_status,
                started_ts_col=pipeline_run_ts_col,
                finished_ts_col=F.lit(datetime.now(timezone.utc)).cast("timestamp"),
                message=end_msg,
            )
        )

    except Exception as e:
        write_log_best_effort(
            run_event(
                spark,
                log_ctx,
                pipeline_run_ts_col=pipeline_run_ts_col,
                batch_id=None,
                status=IngestStatus.FAILED,
                started_ts_col=pipeline_run_ts_col,
                finished_ts_col=F.lit(datetime.now(timezone.utc)).cast("timestamp"),
                message=str(e)[:4000],
                error_class=e.__class__.__name__,
            )
        )
        raise


def main() -> None:
    """Entry point."""
    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark)


if __name__ == "__main__":
    main()
