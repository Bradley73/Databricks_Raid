# ops/ingest_log_context.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Mapping, Any


@dataclass(frozen=True)
class LogContext:
    """
    Pipeline-wide log context (stable metadata).
    """
    target_table_fqn: str
    layer: str
    pipeline_name: str
    run_id: str

    checkpoint_location: Optional[str] = None
    schema_location: Optional[str] = None

    # merged into every event's context_json (event context overrides keys)
    base_context: Optional[Mapping[str, Any]] = None
