# raid/schemas/champindex/schema.py
"""
Schema contract for champindex.

Aligned to the "pure Auto Loader" ingestion method:
- Auto Loader enforces DATA_SCHEMA at read time via .schema(DATA_SCHEMA)
- Auto Loader populates _rescued_data via option("cloudFiles.rescuedDataColumn", "_rescued_data")
- Bronze metadata columns are added in the ingest notebook (source_file/account/snapshot_*/schema_version)

Important:
- _rescued_data is a STRING column in bronze (JSON text emitted by Auto Loader for data that didn't fit the read schema).
- This module intentionally does NOT contain a custom "apply_bronze_schema_with_rescue" function anymore.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DateType
)

SCHEMA_VERSION = "1.0"

# ─────────────────────────────
# 1) Dataset contract (business fields only)
#    Used directly by Auto Loader: .schema(DATA_SCHEMA)
# ─────────────────────────────
DATA_SCHEMA = StructType([
    StructField("AccountName", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("HeroID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Rank", IntegerType(), True),
    StructField("Level", IntegerType(), True),
    StructField("EmpowerLevel", IntegerType(), True),
    StructField("Rarity", IntegerType(), True),
    StructField("Affinity", IntegerType(), True),
    StructField("Faction", IntegerType(), True),

    StructField("UsedT1MasScrolls", IntegerType(), True),
    StructField("UnUsedT1MasScrolls", IntegerType(), True),
    StructField("UsedT2MasScrolls", IntegerType(), True),
    StructField("UnUsedT2MasScrolls", IntegerType(), True),
    StructField("UsedT3MasScrolls", IntegerType(), True),
    StructField("UnUsedT3MasScrolls", IntegerType(), True),

    StructField("HP", IntegerType(), True),
    StructField("ATK", IntegerType(), True),
    StructField("DEF", IntegerType(), True),
    StructField("CritRate", IntegerType(), True),
    StructField("CritDamage", IntegerType(), True),
    StructField("SPD", IntegerType(), True),
    StructField("ACC", IntegerType(), True),
    StructField("RES", IntegerType(), True),

    StructField("BlessingID", IntegerType(), True),
    StructField("BlessingGrade", IntegerType(), True),
])

DATA_COLS = [f.name for f in DATA_SCHEMA.fields]

# ─────────────────────────────
# 2) Bronze-only fields (added after read in ingest pipeline)
# ─────────────────────────────
BRONZE_META_SCHEMA = StructType([
    # Populated by Auto Loader when configured with:
    # .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    StructField("_rescued_data", StringType(), True),

    # Populated by ingest pipeline
    StructField("run_id", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("snapshot_ts", TimestampType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("schema_version", StringType(), True),
])

BRONZE_META_COLS = [f.name for f in BRONZE_META_SCHEMA.fields]

# Full bronze table schema (business + bronze metadata)
BRONZE_TABLE_SCHEMA = StructType(DATA_SCHEMA.fields + BRONZE_META_SCHEMA.fields)
BRONZE_COLS = DATA_COLS + BRONZE_META_COLS

# ─────────────────────────────
# 3) Silver table shape (business only)
# ─────────────────────────────
SILVER_TABLE_SCHEMA = DATA_SCHEMA
SILVER_COLS = DATA_COLS
