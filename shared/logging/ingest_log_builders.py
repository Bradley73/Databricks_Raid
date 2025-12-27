# ops/ingest_log_builders.py

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ops.ingest_log_context import LogContext
from ops.ingest_log_constants import IngestLevel, IngestStatus


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
    spark: SparkSession,
    ctx: LogContext,
    *,
    pipeline_run_ts_col,
    batch_id: Optional[int],
    status: str,
    started_ts_col,
    finished_ts_col=None,
    message: Optional[str] = None,
    error_class: Optional[str] = None,
    event_context: Optional[Mapping[str, Any]] = None,
) -> DataFrame:
    """
    Create a single-row RUN event log.
    Non-null contract fields are always set from ctx + required args.
    """
    base = spark.createDataFrame([{}])

    return (
        base
        .withColumn("target_table_fqn", F.lit(ctx.target_table_fqn))
        .withColumn("layer", F.lit(ctx.layer))
        .withColumn("pipeline_name", F.lit(ctx.pipeline_name))
        .withColumn("event_level", F.lit(IngestLevel.RUN))
        .withColumn("run_id", F.lit(ctx.run_id))
        .withColumn("pipeline_run_ts", pipeline_run_ts_col)
        .withColumn("checkpoint_location", F.lit(ctx.checkpoint_location))
        .withColumn("schema_location", F.lit(ctx.schema_location))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("source_file", F.lit(None))
        .withColumn("row_count", F.lit(None).cast("long"))
        .withColumn("rows_rescued", F.lit(None).cast("long"))
        .withColumn("started_ts", started_ts_col)
        .withColumn("finished_ts", finished_ts_col)
        .withColumn("status", F.lit(status))
        .withColumn("message", F.lit(message))
        .withColumn("error_class", F.lit(error_class))
        .withColumn("context_json", F.lit(_merge_context(ctx, event_context)))
    )


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
