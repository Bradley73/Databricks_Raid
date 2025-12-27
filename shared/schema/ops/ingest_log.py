# schema/ops/ingest_log.py
"""
Ops ingestion log schema contract.

Key design:
- Single source of truth: LOG_SCHEMA
- Derived write contract: LOG_COLS
- target_table_fqn stores the fully-qualified target table name:
    - Unity Catalog: catalog.schema.table
    - Non-UC:        schema.table
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)

TABLE_NAME = "ops.ingest_log"

LOG_SCHEMA = StructType(
    [
        # Identity / ownership
        StructField("target_table_fqn", StringType(), False),   # e.g. "raid.bronze_champindex" or "main.raid.bronze_champindex"
        StructField("layer", StringType(), False),              # e.g. "bronze" / "silver" / "gold" / "ops"
        StructField("pipeline_name", StringType(), False),
        StructField("event_level", StringType(), False),  # "RUN" or "FILE"


        # Run identity
        StructField("run_id", StringType(), False),
        StructField("pipeline_run_ts", TimestampType(), False),

        # Streaming / Auto Loader metadata (nullable for non-streaming)
        StructField("checkpoint_location", StringType(), True),
        StructField("schema_location", StringType(), True),
        StructField("batch_id", LongType(), True),

        # File grain (nullable for run-level events)
        StructField("source_file", StringType(), True),

        # Metrics
        StructField("row_count", LongType(), True),
        StructField("rows_rescued", LongType(), True),

        # Timing
        StructField("started_ts", TimestampType(), False),
        StructField("finished_ts", TimestampType(), True),

        # Outcome / diagnostics
        StructField("status", StringType(), False),             # e.g. "SUCCESS" / "FAILED" / "SKIPPED"
        StructField("message", StringType(), True),
        StructField("error_class", StringType(), True),

        # Extra structured context (JSON stored as string)
        StructField("context_json", StringType(), True),
    ]
)

LOG_COLS: list[str] = [f.name for f in LOG_SCHEMA.fields]


def select_log_cols(df: DataFrame) -> DataFrame:
    """
    Enforce ops.ingest_log write contract:
    - Adds any missing columns as typed NULLs
    - Orders columns per LOG_SCHEMA
    """
    existing = set(df.columns)

    for field in LOG_SCHEMA.fields:
        if field.name not in existing:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

    return df.select(*LOG_COLS)
