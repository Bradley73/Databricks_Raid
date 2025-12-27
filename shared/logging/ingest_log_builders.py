# ops/ingest_log_builders.py

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.logging.ingest_log_context import LogContext
from shared.logging.ingest_log_constants import IngestLevel, IngestStatus


def _to_json_str(d: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not d:
        return None
    return json.dumps(d, separators=(",", ":"), default=str)


def _merge_context(ctx: LogContext, event_context: Optional[Mapping[str, Any]]) -> Optional[str]:
    merged: dict[str, Any] = {}
    if ctx.base_context:
        merged.update(ctx.base_context)
    if event_context:
        merged.update(event_context)
    return _to_json_str(merged)


def run_event(
    spark,
    log_ctx,
    pipeline_run_ts_col,
    batch_id: int,
    status,
    started_ts_col,
    finished_ts_col=None,
    message: str | None = None,
    error_class: str | None = None,
):
    df = (spark.range(1)
          .select(F.lit(log_ctx.run_id).alias("run_id"))
          .withColumn("pipeline_run_ts", pipeline_run_ts_col)
          .withColumn("batch_id", F.lit(int(batch_id)))
          .withColumn("status", F.lit(str(status)))
          .withColumn("started_ts", started_ts_col)
    )

    # Only set finished_ts if provided (STARTED won't have it)
    if finished_ts_col is not None:
        df = df.withColumn("finished_ts", finished_ts_col)
    else:
        df = df.withColumn("finished_ts", F.lit(None).cast("timestamp"))

    if message is not None:
        df = df.withColumn("message", F.lit(message))
    else:
        df = df.withColumn("message", F.lit(None).cast("string"))

    if error_class is not None:
        df = df.withColumn("error_class", F.lit(error_class))
    else:
        df = df.withColumn("error_class", F.lit(None).cast("string"))

    return df


def file_success_events(
    df: DataFrame,
    ctx: LogContext,
    *,
    pipeline_run_ts_col,
    batch_id: Optional[int],
    started_ts_col,
    finished_ts_col,
    rescued_col: str = "_rescued_data",
) -> DataFrame:
    """
    Create FILE-level SUCCESS events (one row per source_file).
    """
    rows_rescued_expr = (
        F.sum(F.when(F.col(rescued_col).isNotNull(), 1).otherwise(0)).cast("long")
    )

    return (
        df.groupBy("source_file")
        .agg(
            F.count("*").cast("long").alias("row_count"),
            rows_rescued_expr.alias("rows_rescued"),
        )
        .withColumn("target_table_fqn", F.lit(ctx.target_table_fqn))
        .withColumn("layer", F.lit(ctx.layer))
        .withColumn("pipeline_name", F.lit(ctx.pipeline_name))
        .withColumn("event_level", F.lit(IngestLevel.FILE))
        .withColumn("run_id", F.lit(ctx.run_id))
        .withColumn("pipeline_run_ts", pipeline_run_ts_col)
        .withColumn("checkpoint_location", F.lit(ctx.checkpoint_location))
        .withColumn("schema_location", F.lit(ctx.schema_location))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("started_ts", started_ts_col)
        .withColumn("finished_ts", finished_ts_col)
        .withColumn("status", F.lit(IngestStatus.SUCCESS))
        .withColumn("message", F.lit(None))
        .withColumn("error_class", F.lit(None))
        .withColumn("context_json", F.lit(_merge_context(ctx, None)))
    )
