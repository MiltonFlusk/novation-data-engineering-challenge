import os
import yaml
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, upper, trim, to_date
from pyspark.sql.types import DecimalType

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


# ==================================================
# CONFIG LOADING
# ==================================================

def load_config():
    config_path = os.environ.get(
        "PIPELINE_CONFIG",
        "config/pipeline_config.yaml"   # fallback for local
    )

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_dq_rules(config):
    dq_path = config.get("dq", {}).get(
        "rules_path",
        "config/dq_rules.yaml"
    )

    with open(dq_path, "r") as f:
        return yaml.safe_load(f)


# ==================================================
# SPARK SESSION
# ==================================================

def create_spark_session(config):
    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ==================================================
# PATH HELPERS
# ==================================================

def path_join(base, sub):
    return f"{base}/{sub}"


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


DATA_ROOT = get_data_root()
BRONZE_DIR = DATA_ROOT / "output" / "bronze"
SILVER_DIR = DATA_ROOT / "output" / "silver"

# ==================================================
# STANDARDISATION
# ==================================================


def standardise_accounts(df):
    df = df.dropDuplicates(["account_id"])

    if "account_status" in df.columns:
        df = df.withColumn("account_status", upper(
            trim(col("account_status"))))

    if "account_type" in df.columns:
        df = df.withColumn("account_type", upper(trim(col("account_type"))))

    if "open_date" in df.columns:
        df = df.withColumn("open_date", to_date(col("open_date")))

    return df


def standardise_customers(df):
    df = df.dropDuplicates(["customer_id"])

    for field in ["first_name", "last_name", "email"]:
        if field in df.columns:
            df = df.withColumn(field, trim(col(field)))

    if "date_of_birth" in df.columns:
        df = df.withColumn("date_of_birth", to_date(col("date_of_birth")))

    return df


def standardise_transactions(df):
    df = df.dropDuplicates(["transaction_id"])

    if "transaction_date" in df.columns:
        df = df.withColumn("transaction_date",
                           to_date(col("transaction_date")))

    if "amount" in df.columns:
        df = df.withColumn("amount", col("amount").cast(DecimalType(18, 2)))

    if "currency" in df.columns:
        df = df.withColumn("currency", upper(trim(col("currency"))))

    if "transaction_type" in df.columns:
        df = df.withColumn("transaction_type", upper(
            trim(col("transaction_type"))))

    return df


# ==================================================
# DQ FLAGGING (CONFIG-DRIVEN)
# ==================================================

def apply_dq_rules(df, dq_rules, accounts_df):
    df = df.withColumn("dq_flag", lit(None))

    # NULL checks
    for field in dq_rules.get("null_checks", {}).get("transactions", []):
        df = df.withColumn(
            "dq_flag",
            when(col(field).isNull(), "NULL_REQUIRED").otherwise(col("dq_flag"))
        )

    # Type checks
    type_checks = dq_rules.get("type_checks", {}).get("transactions", {})
    for field, dtype in type_checks.items():
        if dtype == "decimal":
            df = df.withColumn(
                "dq_flag",
                when(col(field).cast("double").isNull(), "TYPE_MISMATCH")
                .otherwise(col("dq_flag"))
            )

    # Currency normalisation
    currency_rules = dq_rules.get("currency_normalisation", {})
    target = currency_rules.get("target_value", "ZAR")
    accepted = currency_rules.get("accepted_values", [])

    df = df.withColumn(
        "dq_flag",
        when(~col("currency").isin(accepted), "CURRENCY_VARIANT")
        .otherwise(col("dq_flag"))
    )

    df = df.withColumn("currency", upper(trim(col("currency"))))
    df = df.withColumn("currency", when(col("currency").isin(
        accepted), target).otherwise(col("currency")))

    # Referential integrity
    valid_accounts = accounts_df.select("account_id").distinct()

    df = df.join(valid_accounts, on="account_id", how="left")

    df = df.withColumn(
        "dq_flag",
        when(col("account_id").isNull(), "ORPHANED_ACCOUNT")
        .otherwise(col("dq_flag"))
    )

    return df


# ==================================================
# WRITE
# ==================================================

def write_output(df, path):
    (
        df.write
        .mode("overwrite")
        .option("compression", "uncompressed")
        .parquet(str(path))
    )


# ==================================================
# MAIN
# ==================================================

def run_transformation():
    print("Starting Silver transformation")

    config = load_config()
    dq_rules = load_dq_rules(config)

    spark = (
        SparkSession.builder
        .appName("Silver Transformation")
        .master("local[2]")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.hostname", "localhost")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    data_root = get_data_root()

    bronze_base = str(data_root / "output" / "bronze")
    silver_base = str(data_root / "output" / "silver")

    accounts = spark.read.parquet(path_join(bronze_base, "accounts"))
    customers = spark.read.parquet(path_join(bronze_base, "customers"))
    transactions = spark.read.parquet(path_join(bronze_base, "transactions"))

    accounts_clean = standardise_accounts(accounts)
    customers_clean = standardise_customers(customers)
    transactions_clean = standardise_transactions(transactions)

    transactions_clean = apply_dq_rules(
        transactions_clean, dq_rules, accounts_clean)

    write_output(accounts_clean, path_join(silver_base, "accounts"))
    write_output(customers_clean, path_join(silver_base, "customers"))
    write_output(transactions_clean, path_join(silver_base, "transactions"))

    spark.stop()

    print("Silver transformation complete")


if __name__ == "__main__":
    run_transformation()
