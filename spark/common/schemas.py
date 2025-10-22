# spark/common/schemas.py
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# Tolerant Bronze schema — keep most fields as strings to avoid ingestion breakage
bronze_raw_schema = StructType([
    StructField("type",       StringType(), True),
    StructField("product_id", StringType(), True),   # -> symbol
    StructField("time",       StringType(), True),   # -> event_time (parsed)
    StructField("price",      StringType(), True),
    StructField("size",       StringType(), True),
    StructField("side",       StringType(), True),
    StructField("trade_id",   LongType(),   True),

    # Ticker-ish, optional
    StructField("best_bid",   StringType(), True),
    StructField("best_ask",   StringType(), True),
    StructField("volume_24h", StringType(), True),
    StructField("sequence",   LongType(),   True),
])
