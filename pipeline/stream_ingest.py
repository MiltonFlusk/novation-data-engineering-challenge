"""
Stage 3 streaming ingestion.

Processes JSONL micro-batch files from:
    /data/stream/

Writes Delta tables to:
    /data/output/stream_gold/current_balances/
    /data/output/stream_gold/recent_transactions/

The implementation uses directory polling because the Stage 3 feed is supplied
as micro-batch JSONL files.
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    concat_ws,
    coalesce,
    desc,
    greatest,
    lit,
    max as spark_max,
    row_number,
    sum as spark_sum,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


DATA_ROOT = get_data_root()
STREAM_DIR = DATA_ROOT / "stream"
OUTPUT_DIR = DATA_ROOT / "output"
GOLD_DIR = OUTPUT_DIR / "gold"
STREAM_GOLD_DIR = OUTPUT_DIR / "stream_gold"

DIM_ACCOUNTS_PATH = GOLD_DIR / "dim_accounts"
CURRENT_BALANCES_PATH = STREAM_GOLD_DIR / "current_balances"
RECENT_TRANSACTIONS_PATH = STREAM_GOLD_DIR / "recent_transactions"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Stage 3 Stream Ingestion")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.hostname", "localhost")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def delta_exists(path):
    return (Path(path) / "_delta_log").exists()


def stream_schema():
    location_schema = StructType(
        [
            StructField("province", StringType(), True),
            StructField("city", StringType(), True),
            StructField("coordinates", StringType(), True),
        ]
    )

    metadata_schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("retry_flag", BooleanType(), True),
        ]
    )

    return StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("transaction_time", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("merchant_subcategory", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("location", location_schema, True),
            StructField("metadata", metadata_schema, True),
        ]
    )


def current_balances_schema():
    return StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("current_balance", DecimalType(18, 2), False),
            StructField("last_transaction_timestamp", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )


def recent_transactions_schema():
    return StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("amount", DecimalType(18, 2), False),
            StructField("transaction_type", StringType(), False),
            StructField("channel", StringType(), True),
            StructField("updated_at", TimestampType(), False),
        ]
    )


def write_delta_overwrite(df, path):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("compression", "uncompressed")
        .save(str(path))
    )


def create_empty_tables(spark):
    STREAM_GOLD_DIR.mkdir(parents=True, exist_ok=True)

    if not delta_exists(CURRENT_BALANCES_PATH):
        empty_current = spark.sql(
            """
            SELECT
                CAST(NULL AS STRING) AS account_id,
                CAST(NULL AS DECIMAL(18,2)) AS current_balance,
                CAST(NULL AS TIMESTAMP) AS last_transaction_timestamp,
                CAST(NULL AS TIMESTAMP) AS updated_at
            WHERE 1 = 0
            """
        )

        write_delta_overwrite(empty_current, CURRENT_BALANCES_PATH)
        print(f"Created empty current_balances table at: {CURRENT_BALANCES_PATH}")

    if not delta_exists(RECENT_TRANSACTIONS_PATH):
        empty_recent = spark.sql(
            """
            SELECT
                CAST(NULL AS STRING) AS account_id,
                CAST(NULL AS STRING) AS transaction_id,
                CAST(NULL AS TIMESTAMP) AS transaction_timestamp,
                CAST(NULL AS DECIMAL(18,2)) AS amount,
                CAST(NULL AS STRING) AS transaction_type,
                CAST(NULL AS STRING) AS channel,
                CAST(NULL AS TIMESTAMP) AS updated_at
            WHERE 1 = 0
            """
        )

        write_delta_overwrite(empty_recent, RECENT_TRANSACTIONS_PATH)
        print(f"Created empty recent_transactions table at: {RECENT_TRANSACTIONS_PATH}")


def list_stream_files():
    if not STREAM_DIR.exists():
        return []

    return sorted(STREAM_DIR.glob("stream_*.jsonl"))


def load_dim_accounts(spark):
    if not delta_exists(DIM_ACCOUNTS_PATH):
        raise FileNotFoundError(
            f"dim_accounts Delta table not found at {DIM_ACCOUNTS_PATH}. "
            "Run the batch pipeline before Stage 3 streaming."
        )

    return (
        spark.read
        .format("delta")
        .load(str(DIM_ACCOUNTS_PATH))
        .select(
            col("account_id"),
            col("current_balance").cast(DecimalType(18, 2)).alias("snapshot_balance"),
        )
    )


def clean_events(spark, stream_file, processed_at):
    raw = spark.read.schema(stream_schema()).json(str(stream_file))

    events = (
        raw
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("account_id", trim(col("account_id")))
        .withColumn("transaction_type", upper(trim(col("transaction_type"))))
        .withColumn("amount", col("amount").cast(DecimalType(18, 2)))
        .withColumn(
            "transaction_timestamp",
            to_timestamp(concat_ws(" ", col("transaction_date"), col("transaction_time"))),
        )
        .withColumn("updated_at", lit(processed_at).cast(TimestampType()))
        .where(col("transaction_id").isNotNull())
        .where(col("account_id").isNotNull())
        .where(col("transaction_timestamp").isNotNull())
        .where(col("amount").isNotNull())
        .where(col("transaction_type").isin("DEBIT", "CREDIT", "FEE", "REVERSAL"))
        .dropDuplicates(["account_id", "transaction_id"])
    )

    return events


def add_balance_delta(events):
    amount_abs = spark_abs(col("amount")).cast(DecimalType(18, 2))

    return events.withColumn(
        "balance_delta",
        when(col("transaction_type").isin("CREDIT", "REVERSAL"), amount_abs)
        .otherwise(-amount_abs)
        .cast(DecimalType(18, 2)),
    )


def update_current_balances(spark, events, dim_accounts):
    events_with_delta = add_balance_delta(events)

    batch_updates = (
        events_with_delta
        .groupBy("account_id")
        .agg(
            spark_sum("balance_delta").cast(DecimalType(18, 2)).alias("batch_delta"),
            spark_max("transaction_timestamp").alias("batch_last_transaction_timestamp"),
            spark_max("updated_at").alias("updated_at"),
        )
    )

    existing = (
        spark.read
        .format("delta")
        .load(str(CURRENT_BALANCES_PATH))
        .select(
            col("account_id"),
            col("current_balance").alias("existing_balance"),
            col("last_transaction_timestamp").alias("existing_last_transaction_timestamp"),
        )
    )

    zero_decimal = lit(0).cast(DecimalType(18, 2))

    source_updates = (
        batch_updates.alias("b")
        .join(existing.alias("e"), col("b.account_id") == col("e.account_id"), "left")
        .join(dim_accounts.alias("a"), col("b.account_id") == col("a.account_id"), "left")
        .select(
            col("b.account_id").alias("account_id"),
            (
                coalesce(col("e.existing_balance"), col("a.snapshot_balance"), zero_decimal)
                + col("b.batch_delta")
            ).cast(DecimalType(18, 2)).alias("current_balance"),
            greatest(
                coalesce(
                    col("e.existing_last_transaction_timestamp"),
                    col("b.batch_last_transaction_timestamp"),
                ),
                col("b.batch_last_transaction_timestamp"),
            ).alias("last_transaction_timestamp"),
            col("b.updated_at").alias("updated_at"),
        )
    )

    target = DeltaTable.forPath(spark, str(CURRENT_BALANCES_PATH))

    (
        target.alias("target")
        .merge(
            source_updates.alias("source"),
            "target.account_id = source.account_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def update_recent_transactions(spark, events):
    new_recent = events.select(
        col("account_id"),
        col("transaction_id"),
        col("transaction_timestamp"),
        col("amount").cast(DecimalType(18, 2)).alias("amount"),
        col("transaction_type"),
        col("channel"),
        col("updated_at"),
    )

    target = DeltaTable.forPath(spark, str(RECENT_TRANSACTIONS_PATH))

    (
        target.alias("target")
        .merge(
            new_recent.alias("source"),
            """
            target.account_id = source.account_id
            AND target.transaction_id = source.transaction_id
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    recent = spark.read.format("delta").load(str(RECENT_TRANSACTIONS_PATH))

    recent_window = Window.partitionBy("account_id").orderBy(
        desc("transaction_timestamp"),
        desc("updated_at"),
        desc("transaction_id"),
    )

    retained = (
        recent
        .withColumn("recent_rank", row_number().over(recent_window))
        .where(col("recent_rank") <= 50)
        .drop("recent_rank")
        .localCheckpoint(eager=True)
    )

    write_delta_overwrite(retained, RECENT_TRANSACTIONS_PATH)


def process_file(spark, stream_file, dim_accounts):
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"Processing stream file: {stream_file.name}")

    events = clean_events(spark, stream_file, processed_at)

    valid_events = (
        events.alias("s")
        .join(
            dim_accounts.select("account_id").alias("a"),
            col("s.account_id") == col("a.account_id"),
            "inner",
        )
        .select("s.*")
    )

    valid_events = valid_events.persist(StorageLevel.MEMORY_AND_DISK)
    record_count = valid_events.count()

    if record_count == 0:
        print(f"No valid records in {stream_file.name}")
        valid_events.unpersist()
        return 0

    update_current_balances(spark, valid_events, dim_accounts)
    update_recent_transactions(spark, valid_events)

    valid_events.unpersist()

    print(f"Processed {record_count} records from {stream_file.name}")

    return record_count


def run_stream_ingestion():
    poll_interval = int(os.getenv("STREAM_POLL_INTERVAL_SECONDS", "10"))
    quiesce_seconds = int(os.getenv("STREAM_QUIESCE_SECONDS", "60"))
    max_runtime_seconds = int(os.getenv("STREAM_MAX_RUNTIME_SECONDS", "900"))

    print("Starting Stage 3 stream ingestion")
    print(f"STREAM_DIR: {STREAM_DIR}")
    print(f"STREAM_GOLD_DIR: {STREAM_GOLD_DIR}")

    spark = create_spark_session()

    try:
        spark.sparkContext.setCheckpointDir(str(OUTPUT_DIR / "_checkpoints" / "stream_ingest"))

        create_empty_tables(spark)

        if not STREAM_DIR.exists():
            print(f"Stream directory does not exist: {STREAM_DIR}")
            print("Empty stream_gold Delta tables are available.")
            return

        dim_accounts = load_dim_accounts(spark).persist(StorageLevel.MEMORY_AND_DISK)
        dim_accounts.count()

        start_time = time.time()
        last_file_time = time.time()
        processed_files = set()
        total_records = 0

        while True:
            stream_files = list_stream_files()
            new_files = [file_path for file_path in stream_files if file_path.name not in processed_files]

            if new_files:
                for stream_file in new_files:
                    total_records += process_file(spark, stream_file, dim_accounts)
                    processed_files.add(stream_file.name)
                    last_file_time = time.time()
            else:
                quiet_time = time.time() - last_file_time
                elapsed_time = time.time() - start_time

                if quiet_time >= quiesce_seconds:
                    print(f"No new stream files for {int(quiet_time)} seconds. Ending stream ingestion.")
                    break

                if elapsed_time >= max_runtime_seconds:
                    print(f"Maximum stream runtime reached after {int(elapsed_time)} seconds.")
                    break

                time.sleep(poll_interval)

        dim_accounts.unpersist()

        print("Stage 3 stream ingestion complete.")
        print(f"Files processed: {len(processed_files)}")
        print(f"Records processed: {total_records}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_stream_ingestion()