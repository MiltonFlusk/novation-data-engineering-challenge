from importlib.resources import path
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


# ==================================================
# PART 1: PROJECT PATHS
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


DATA_ROOT = get_data_root()
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output" / "bronze"

ACCOUNTS_FILE = INPUT_DIR / "accounts.csv"
CUSTOMERS_FILE = INPUT_DIR / "customers.csv"
TRANSACTIONS_FILE = INPUT_DIR / "transactions.jsonl"

# ==================================================
# PART 2: ENVIRONMENT CHECKS
# ==================================================


def check_environment():
    print("\n=== ENVIRONMENT CHECK ===")

    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("INPUT_DIR:", INPUT_DIR)
    print("OUTPUT_DIR:", OUTPUT_DIR)

    print("\nFiles:")
    print("accounts.csv:", ACCOUNTS_FILE.exists(), ACCOUNTS_FILE)
    print("customers.csv:", CUSTOMERS_FILE.exists(), CUSTOMERS_FILE)
    print("transactions.jsonl:", TRANSACTIONS_FILE.exists(), TRANSACTIONS_FILE)

    print("\nEnvironment variables:")
    print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
    print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
    print("SPARK_HOME:", os.environ.get("SPARK_HOME"))

    java_home = os.environ.get("JAVA_HOME")
    hadoop_home = os.environ.get("HADOOP_HOME")
    spark_home = os.environ.get("SPARK_HOME")

    if java_home:
        print("java.exe exists:", Path(java_home, "bin", "java.exe").exists())

    if hadoop_home:
        print("winutils.exe exists:", Path(
            hadoop_home, "bin", "winutils.exe").exists())

    if spark_home:
        print("spark-submit.cmd exists:",
              Path(spark_home, "bin", "spark-submit.cmd").exists())

    print("=== ENVIRONMENT CHECK COMPLETE ===\n")


# ==================================================
# PART 3: CREATE SPARK SESSION
# ==================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Bronze Ingestion")
        .master("local[2]")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.hostname", "localhost")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

# ==================================================
# PART 4: HELPER FUNCTIONS
# ==================================================


def add_metadata(df, source_file):
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(source_file))
    )


def write_bronze(df, output_path, table_name):
    print(f"Writing {table_name} to: {output_path}")

    df.write \
        .mode("overwrite") \
        .option("compression", "uncompressed") \
        .parquet(str(output_path))

    print(f"{table_name} written successfully.\n")

# ==================================================
# PART 5: INGEST DATASETS
# ==================================================


def ingest_accounts(spark):
    print("INGESTING ACCOUNTS")

    df = spark.read.csv(str(ACCOUNTS_FILE), header=True)
    df = add_metadata(df, "accounts.csv")

    write_bronze(df, OUTPUT_DIR / "accounts", "accounts")


def ingest_customers(spark):
    print("INGESTING CUSTOMERS")

    df = spark.read.csv(str(CUSTOMERS_FILE), header=True)
    df = add_metadata(df, "customers.csv")

    write_bronze(df, OUTPUT_DIR / "customers", "customers")


def ingest_transactions(spark):
    print("INGESTING TRANSACTIONS")

    df = spark.read.json(str(TRANSACTIONS_FILE))
    df = add_metadata(df, "transactions.jsonl")

    write_bronze(df, OUTPUT_DIR / "transactions", "transactions")
# ==================================================
# PART 6: MAIN
# ==================================================


def main():
    print("Starting Bronze ingestion...\n")

    check_environment()

    spark = create_spark_session()

    try:
        ingest_accounts(spark)
    except Exception as e:
        print(f"Accounts failed: {e}")

    try:
        ingest_customers(spark)
    except Exception as e:
        print(f"Customers failed: {e}")

    try:
        ingest_transactions(spark)
    except Exception as e:
        print(f"Transactions failed: {e}")

    spark.stop()

    print("Bronze ingestion complete.")


if __name__ == "__main__":
    main()
