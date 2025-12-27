# ops/ingest_logging.py

from __future__ import annotations

from pyspark.sql import DataFrame

from schema.ops.ingest_log import select_log_cols, TABLE_NAME as INGEST_LOG_TABLE
from ops.logging_utils import get_logger

logger = get_logger("ingest_logging")


def write_log_best_effort(log_df: DataFrame) -> None:
    """
    Best-effort write to ops.ingest_log.

    - Never raises
    - Never masks caller exceptions
    - Uses platform logging when available
    """
    try:
        (
            select_log_cols(log_df)
            .write
            .mode("append")
            .format("delta")
            .saveAsTable(INGEST_LOG_TABLE)
        )
        logger.info("Successfully wrote ingest log record")

    except Exception as log_error:
        # Do NOT raise — logging must never break ingestion
        logger.warn("Failed to write ingest log record")
        logger.warn(str(log_error))
