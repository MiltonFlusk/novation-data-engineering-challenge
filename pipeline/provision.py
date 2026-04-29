import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_date,
    date_format,
    floor,
    lit,
    months_between,
    row_number,
    to_date,
    to_timestamp,
    upper,
    when,
)
from pyspark.sql.window import Window

# ==================================================
# CONFIG
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


def resolve_path(path_value):
    path_value = str(path_value).replace("\\", "/")

    if path_value.startswith("/data/"):
        if Path("/data").exists():
            return path_value

        return str(PROJECT_ROOT / path_value.lstrip("/"))

    if path_value.startswith("data/"):
        relative_part = path_value.replace("data/", "", 1)
        return str(get_data_root() / relative_part)

    return str(PROJECT_ROOT / path_value)


ALLOWED_DQ_FLAGS = [
    "ORPHANED_ACCOUNT",
    "DUPLICATE_DEDUPED",
    "TYPE_MISMATCH",
    "DATE_FORMAT",
    "CURRENCY_VARIANT",
    "NULL_REQUIRED",
]

DQ_REPORT_MAPPING = {
    "ORPHANED_ACCOUNT": ("orphaned_transactions", "QUARANTINED"),
    "DUPLICATE_DEDUPED": ("duplicate_transactions", "DEDUPLICATED_KEEP_FIRST"),
    "TYPE_MISMATCH": ("amount_type_mismatch", "CAST_TO_DECIMAL"),
    "DATE_FORMAT": ("date_format_inconsistency", "NORMALISED_DATE"),
    "CURRENCY_VARIANT": ("currency_variants", "NORMALISED_CURRENCY"),
    "NULL_REQUIRED": ("null_account_id", "EXCLUDED_NULL_PK"),
}

EXCLUDED_ACTIONS = {
    "QUARANTINED",
    "EXCLUDED_CAST_FAILED",
    "EXCLUDED_NULL_PK",
}


def load_config():
    config_path = os.environ.get(
        "PIPELINE_CONFIG", "config/pipeline_config.yaml")

    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


def resolve_path(path_value):
    path_value = str(path_value).replace("\\", "/")

    if path_value.startswith("/data/"):
        return path_value

    if path_value.startswith("data/"):
        relative_part = path_value.replace("data/", "", 1)
        return str(get_data_root() / relative_part)

    return str(PROJECT_ROOT / path_value)


def get_config_value(config, sections_and_keys, default=None):
    for section_name, key_name in sections_and_keys:
        section = config.get(section_name, {})
        if isinstance(section, dict) and key_name in section:
            return section[key_name]

    return default


# ==================================================
# SPARK
# ==================================================


def create_spark_session(config=None):
    builder = (
        SparkSession.builder
        .appName("Gold Provisioning")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.hostname", "localhost")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


# ==================================================
# READ / WRITE HELPERS
# ==================================================


def read_table(spark, path):
    """Read Delta when available; fall back to Parquet for current local Silver outputs."""
    try:
        return spark.read.format("delta").load(str(path))
    except Exception:
        return spark.read.parquet(str(path))


def remove_existing_path(path):
    path_obj = Path(path)
    if path_obj.exists():
        shutil.rmtree(path_obj)


def write_delta(df, path, table_name):
    print(f"Writing {table_name} to: {path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("compression", "uncompressed")
        .save(str(path))
    )

    print(f"{table_name} written successfully.")


# ==================================================
# SMALL COLUMN HELPERS
# ==================================================


def safe_col(df, alias_name, column_name, data_type="string", default_value=None):
    if column_name in df.columns:
        return col(f"{alias_name}.{column_name}").cast(data_type)

    return lit(default_value).cast(data_type)


def safe_date_col(df, alias_name, column_name):
    if column_name in df.columns:
        return to_date(col(f"{alias_name}.{column_name}"))

    return lit(None).cast("date")


def standardised_dq_flag(df, alias_name=None):
    if "dq_flag" not in df.columns:
        return lit(None).cast("string")

    if alias_name:
        dq_column = col(f"{alias_name}.dq_flag")
    else:
        dq_column = col("dq_flag")

    flag = upper(dq_column.cast("string"))

    return when(flag.isin(ALLOWED_DQ_FLAGS), flag).otherwise(lit(None).cast("string"))

# ==================================================
# GOLD TABLES
# ==================================================


def build_dim_customers(customers_df):
    deduped = (
        customers_df
        .dropDuplicates(["customer_id"])
        .where(col("customer_id").isNotNull())
    )

    dob_date = coalesce(
        to_date(col("dob")),
        to_date(col("dob"), "yyyy-MM-dd"),
        to_date(col("dob"), "dd/MM/yyyy"),
        to_date(col("dob"), "MM/dd/yyyy"),
    )

    age = floor(months_between(current_date(), dob_date) / lit(12))

    base = (
        deduped
        .withColumn("_dob_date", dob_date)
        .withColumn("_age", age)
        .withColumn(
            "age_band",
            when(col("_age") >= 65, lit("65+"))
            .when(col("_age") >= 56, lit("56-65"))
            .when(col("_age") >= 46, lit("46-55"))
            .when(col("_age") >= 36, lit("36-45"))
            .when(col("_age") >= 26, lit("26-35"))
            .when(col("_age") >= 18, lit("18-25"))
            .otherwise(lit(None).cast("string"))
        )
    )

    window_spec = Window.orderBy("customer_id")

    return (
        base
        .withColumn("customer_sk", row_number().over(window_spec).cast("long"))
        .select(
            col("customer_sk"),
            col("customer_id").cast("string"),
            col("gender").cast("string"),
            col("province").cast("string"),
            col("income_band").cast("string"),
            col("segment").cast("string"),
            col("risk_score").cast("int"),
            col("kyc_status").cast("string"),
            col("age_band").cast("string"),
        )
    )


def build_dim_accounts(accounts_df, dim_customers):
    customer_ref_col = "customer_ref" if "customer_ref" in accounts_df.columns else "customer_id"

    accounts_base = (
        accounts_df
        .dropDuplicates(["account_id"])
        .where(col("account_id").isNotNull())
        .withColumn("customer_id", col(customer_ref_col).cast("string"))
        .where(col("customer_id").isNotNull())
        .alias("a")
    )

    valid_customers = dim_customers.select(
        "customer_id").dropDuplicates().alias("c")

    joined = accounts_base.join(
        valid_customers,
        col("a.customer_id") == col("c.customer_id"),
        "inner",
    )

    window_spec = Window.orderBy(col("a.account_id"))

    return (
        joined
        .withColumn("account_sk", row_number().over(window_spec).cast("long"))
        .select(
            col("account_sk"),
            col("a.account_id").cast("string").alias("account_id"),
            col("a.customer_id").cast("string").alias("customer_id"),
            safe_col(accounts_df, "a", "account_type",
                     "string").alias("account_type"),
            safe_col(accounts_df, "a", "account_status",
                     "string").alias("account_status"),
            safe_date_col(accounts_df, "a", "open_date").alias("open_date"),
            safe_col(accounts_df, "a", "product_tier",
                     "string").alias("product_tier"),
            safe_col(accounts_df, "a", "digital_channel",
                     "string").alias("digital_channel"),
            safe_col(accounts_df, "a", "credit_limit",
                     "decimal(18,2)").alias("credit_limit"),
            safe_col(accounts_df, "a", "current_balance",
                     "decimal(18,2)").alias("current_balance"),
            safe_date_col(accounts_df, "a", "last_activity_date").alias(
                "last_activity_date"),
        )
    )


def build_fact_transactions(transactions_df, dim_accounts, dim_customers):
    transactions_base = (
        transactions_df
        .dropDuplicates(["transaction_id"])
        .where(col("transaction_id").isNotNull())
        .alias("t")
    )

    accounts_lookup = (
        dim_accounts
        .select("account_sk", "account_id", "customer_id")
        .alias("a")
    )

    customers_lookup = (
        dim_customers
        .select("customer_sk", "customer_id")
        .alias("c")
    )

    joined = (
        transactions_base
        .join(accounts_lookup, col("t.account_id") == col("a.account_id"), "inner")
        .join(customers_lookup, col("a.customer_id") == col("c.customer_id"), "inner")
    )

    if "transaction_time" in transactions_df.columns:
        transaction_timestamp = coalesce(
            to_timestamp(
                concat_ws(
                    " ",
                    date_format(to_date(col("t.transaction_date")),
                                "yyyy-MM-dd"),
                    col("t.transaction_time"),
                )
            ),
            to_timestamp(to_date(col("t.transaction_date")).cast("string")),
        )
    else:
        transaction_timestamp = to_timestamp(
            to_date(col("t.transaction_date")).cast("string"))

    if "merchant_subcategory" in transactions_df.columns:
        merchant_subcategory = col("t.merchant_subcategory").cast("string")
    else:
        merchant_subcategory = lit(None).cast("string")

    if "location" in transactions_df.columns:
        province = col("t.location.province").cast("string")
    elif "province" in transactions_df.columns:
        province = col("t.province").cast("string")
    else:
        province = lit(None).cast("string")

    base = joined.select(
        col("t.transaction_id").cast("string").alias("transaction_id"),
        col("a.account_sk").cast("long").alias("account_sk"),
        col("c.customer_sk").cast("long").alias("customer_sk"),
        to_date(col("t.transaction_date")).alias("transaction_date"),
        transaction_timestamp.alias("transaction_timestamp"),
        upper(col("t.transaction_type").cast(
            "string")).alias("transaction_type"),
        safe_col(transactions_df, "t", "merchant_category",
                 "string").alias("merchant_category"),
        merchant_subcategory.alias("merchant_subcategory"),
        safe_col(transactions_df, "t", "amount",
                 "decimal(18,2)").alias("amount"),
        lit("ZAR").cast("string").alias("currency"),
        upper(safe_col(transactions_df, "t", "channel", "string")).alias("channel"),
        province.alias("province"),
        standardised_dq_flag(transactions_df, "t").alias("dq_flag"),
        col("t.ingestion_timestamp").cast(
            "timestamp").alias("ingestion_timestamp"),
    )

    window_spec = Window.orderBy("transaction_id")

    return (
        base
        .withColumn("transaction_sk", row_number().over(window_spec).cast("long"))
        .select(
            col("transaction_sk"),
            col("transaction_id"),
            col("account_sk"),
            col("customer_sk"),
            col("transaction_date"),
            col("transaction_timestamp"),
            col("transaction_type"),
            col("merchant_category"),
            col("merchant_subcategory"),
            col("amount"),
            col("currency"),
            col("channel"),
            col("province"),
            col("dq_flag"),
            col("ingestion_timestamp"),
        )
    )


# ==================================================
# DQ REPORT
# ==================================================


def utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_count_csv(spark, path, fallback_count=0):
    path_obj = Path(path)
    if not path_obj.exists():
        return int(fallback_count)

    return spark.read.option("header", True).csv(str(path_obj)).count()


def safe_count_json(spark, path, fallback_count=0):
    path_obj = Path(path)
    if not path_obj.exists():
        return int(fallback_count)

    return spark.read.json(str(path_obj)).count()


def get_input_counts(spark, config, accounts_df, customers_df, transactions_df):
    input_config = config.get("input", {}) if isinstance(
        config.get("input", {}), dict) else {}

    accounts_path = input_config.get(
        "accounts_path", "data/input/accounts.csv")
    customers_path = input_config.get(
        "customers_path", "data/input/customers.csv")
    transactions_path = input_config.get(
        "transactions_path", "data/input/transactions.jsonl")

    accounts_count = safe_count_csv(
        spark,
        resolve_path(accounts_path),
        accounts_df.count(),
    )

    customers_count = safe_count_csv(
        spark,
        resolve_path(customers_path),
        customers_df.count(),
    )

    transactions_count = safe_count_json(
        spark,
        resolve_path(transactions_path),
        transactions_df.count(),
    )

    return {
        "accounts_raw": accounts_count,
        "transactions_raw": transactions_count,
        "customers_raw": customers_count,
    }


def build_dq_issues(transactions_df, source_record_counts):
    if "dq_flag" not in transactions_df.columns:
        return []

    flagged = (
        transactions_df
        .withColumn("_dq_flag", standardised_dq_flag(transactions_df))
        .where(col("_dq_flag").isNotNull())
        .groupBy("_dq_flag")
        .count()
        .collect()
    )

    issues = []
    transactions_raw = source_record_counts.get("transactions_raw", 0) or 1
    accounts_raw = source_record_counts.get("accounts_raw", 0) or 1

    for row in flagged:
        dq_flag = row["_dq_flag"]
        records_affected = int(row["count"])
        issue_type, handling_action = DQ_REPORT_MAPPING.get(
            dq_flag,
            (dq_flag.lower(), "UNKNOWN"),
        )

        denominator = accounts_raw if issue_type == "null_account_id" else transactions_raw
        percentage = round((records_affected / denominator) * 100, 2)

        if handling_action in EXCLUDED_ACTIONS:
            records_in_output = 0
        else:
            records_in_output = records_affected

        issues.append(
            {
                "issue_type": issue_type,
                "records_affected": records_affected,
                "percentage_of_total": percentage,
                "handling_action": handling_action,
                "records_in_output": records_in_output,
            }
        )

    return issues


def write_dq_report(
    output_path,
    run_timestamp,
    stage,
    source_record_counts,
    dq_issues,
    gold_layer_record_counts,
    execution_duration_seconds,
):
    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_timestamp,
        "stage": str(stage),
        "source_record_counts": source_record_counts,
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_layer_record_counts,
        "execution_duration_seconds": int(execution_duration_seconds),
    }

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    print(f"DQ report written to: {output_file}")


# ==================================================
# MAIN
# ==================================================


def run_provisioning():
    pipeline_start_time = time.time()
    run_timestamp = utc_timestamp()

    print("Starting Gold provisioning")

    config = load_config()
    spark = create_spark_session(config)

    silver_base = resolve_path(config["output"]["silver_path"])
    gold_base = resolve_path(config["output"]["gold_path"])
    dq_report_path = resolve_path(config["output"]["dq_report_path"])
    stage = os.environ.get("PIPELINE_STAGE", str(config.get("stage", "2")))

    accounts = read_table(spark, f"{silver_base}/accounts")
    customers = read_table(spark, f"{silver_base}/customers")
    transactions = read_table(spark, f"{silver_base}/transactions")

    dim_customers = build_dim_customers(customers)
    dim_accounts = build_dim_accounts(accounts, dim_customers)
    fact_transactions = build_fact_transactions(
        transactions, dim_accounts, dim_customers)

    write_delta(dim_accounts, f"{gold_base}/dim_accounts", "dim_accounts")
    write_delta(dim_customers, f"{gold_base}/dim_customers", "dim_customers")
    write_delta(fact_transactions,
                f"{gold_base}/fact_transactions", "fact_transactions")

    source_record_counts = get_input_counts(
        spark,
        config,
        accounts,
        customers,
        transactions,
    )

    gold_layer_record_counts = {
        "fact_transactions": fact_transactions.count(),
        "dim_accounts": dim_accounts.count(),
        "dim_customers": dim_customers.count(),
    }

    dq_issues = build_dq_issues(transactions, source_record_counts)

    write_dq_report(
        dq_report_path,
        run_timestamp,
        stage,
        source_record_counts,
        dq_issues,
        gold_layer_record_counts,
        time.time() - pipeline_start_time,
    )

    spark.stop()

    print("Gold provisioning complete")


if __name__ == "__main__":
    run_provisioning()
