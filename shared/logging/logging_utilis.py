# ops/logging_utils.py

def get_logger(name: str):
    """
    Returns a logger that works in Databricks Jobs and notebooks.
    Falls back to print-style logging if log4j is unavailable.
    """
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession")

        log4j = spark._jvm.org.apache.log4j
        return log4j.LogManager.getLogger(name)

    except Exception:
        # Fallback logger with same interface
        class FallbackLogger:
            def warn(self, msg):  # log4j naming
                print(f"WARN [{name}] {msg}")

            def error(self, msg):
                print(f"ERROR [{name}] {msg}")

            def info(self, msg):
                print(f"INFO [{name}] {msg}")

        return FallbackLogger()
