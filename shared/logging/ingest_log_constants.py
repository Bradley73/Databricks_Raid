# ops/ingest_log_constants.py

class IngestStatus:
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    EMPTY = "EMPTY"          # optional: for no-op batches
    PARTIAL = "PARTIAL"      # optional: if you ever support partial success


class IngestLevel:
    RUN = "RUN"
    FILE = "FILE"
