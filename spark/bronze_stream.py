# spark/bronze_stream.py
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from common.schemas import bronze_raw_schema
from common.transforms import parse_event_time
from common.io import (
    bronze_trades_path,
    bronze_trades_dlq_path,
    bronze_checkpoint_path,
    bronze_dlq_checkpoint_path,
)

def get_env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def main():
    load_dotenv()

    bootstrap = get_env("BOOTSTRAP_SERVERS", "localhost:9092", required=True)
    topic_raw = get_env("KAFKA_TOPIC_TRADES_RAW", "trades.raw", required=True)
    source_name = get_env("SOURCE_NAME", "coinbase-ws")
    starting_offsets = get_env("STARTING_OFFSETS", "latest")
    max_offsets_per_trigger = get_env("MAX_OFFSETS_PER_TRIGGER")

    spark = (
        SparkSession.builder
        .appName("bronze_stream")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Kafka source
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic_raw)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger:
        kafka_df = kafka_df.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    kafka_df = kafka_df.load()

    # Base metadata
    base = (
        kafka_df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.col("value").cast(StringType()).alias("raw_json"),
        )
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_time"))
        .withColumn("source", F.lit(source_name))
    )

    # -------------------------------------------------------------------------
    # Safe, tolerant parse that doesn’t rely on Spark injecting _corrupt_record
    # -------------------------------------------------------------------------
    parsed = F.from_json(
        F.col("raw_json"),
        bronze_raw_schema,
        {"mode": "PERMISSIVE"},
    )

    enriched = (
        base.withColumn("parsed", parsed)
        # Mark as corrupt when parse fails (parsed == NULL while raw_json exists)
        .withColumn("is_corrupt", F.col("parsed").isNull())
        .withColumn("event_time", parse_event_time(F.col("parsed.time")))
        .withColumn("symbol", F.col("parsed.product_id"))
        .withColumn("event_type", F.col("parsed.type"))
        .withColumn("price", F.col("parsed.price"))
        .withColumn("size", F.col("parsed.size"))
        .withColumn("side", F.col("parsed.side"))
        # Explicit _corrupt_record field for downstream handling / DLQ
        .withColumn(
            "_corrupt_record",
            F.when(F.col("is_corrupt"), F.col("raw_json")).otherwise(
                F.lit(None).cast(StringType())
            ),
        )
    )

    # Split into good and bad branches
    good = enriched.filter(~F.col("is_corrupt"))
    bad = enriched.filter(F.col("is_corrupt"))

    # Final selection
    select_cols = [
        "ingest_time",
        "ingest_date",
        "source",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_json",
        "event_time",
        "symbol",
        "event_type",
        "price",
        "size",
        "side",
        "_corrupt_record",
    ]
    good = good.select(*select_cols)
    bad = bad.select(*select_cols)

    # -------------------------
    # Write streams (Parquet)
    # -------------------------
    good_q = (
        good.writeStream.format("parquet")
        .option("path", bronze_trades_path())
        .option("checkpointLocation", bronze_checkpoint_path())
        .partitionBy("ingest_date")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .start()
    )

    bad_q = (
        bad.writeStream.format("parquet")
        .option("path", bronze_trades_dlq_path())
        .option("checkpointLocation", bronze_dlq_checkpoint_path())
        .partitionBy("ingest_date")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
