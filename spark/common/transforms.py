# spark/common/transforms.py
from pyspark.sql import functions as F

def parse_event_time(col):
    """
    Parse ISO8601 Z time with optional fractional seconds.
    e.g., 2025-10-21T22:15:09Z or 2025-10-21T22:15:09.123456Z
    Returns a timestamp(UTC).
    """
    # Try microseconds, milliseconds, seconds
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
