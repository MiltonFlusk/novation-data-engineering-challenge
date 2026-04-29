import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"

BASE_DIR = Path(__file__).resolve().parents[1]

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
REPORTS_DIR = BASE_DIR / "data" / "reports"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DQ Report")
        .master("local[*]")
        .getOrCreate()
    )


def count_nulls(df):
    result = {}

    for column in df.columns:
        null_count = df.select(
            count(when(col(column).isNull(), column)).alias("null_count")
        ).collect()[0]["null_count"]

        result[column] = null_count

    return result


def table_report(df, primary_key=None):
    total_rows = df.count()

    report = {
        "total_rows": total_rows,
        "columns": df.columns,
        "null_counts": count_nulls(df)
    }

    if primary_key and primary_key in df.columns:
        distinct_count = df.select(primary_key).distinct().count()
        duplicate_count = total_rows - distinct_count

        report["primary_key"] = primary_key
        report["distinct_primary_keys"] = distinct_count
        report["duplicate_primary_keys"] = duplicate_count

    return report


def build_dq_report():
    print("Starting DQ report")

    spark = create_spark_session()

    tables = {
        "silver_customers": {
            "path": SILVER_DIR / "customers",
            "primary_key": "customer_id"
        },
        "silver_accounts": {
            "path": SILVER_DIR / "accounts",
            "primary_key": "account_id"
        },
        "silver_transactions": {
            "path": SILVER_DIR / "transactions",
            "primary_key": "transaction_id"
        },
        "gold_dim_customers": {
            "path": GOLD_DIR / "dim_customers",
            "primary_key": "customer_id"
        },
        "gold_dim_accounts": {
            "path": GOLD_DIR / "dim_accounts",
            "primary_key": "account_id"
        },
        "gold_fact_transactions": {
            "path": GOLD_DIR / "fact_transactions",
            "primary_key": "transaction_id"
        }
    }

    dq_report = {}

    for table_name, details in tables.items():
        path = details["path"]
        primary_key = details["primary_key"]

        if path.exists():
            df = spark.read.parquet(str(path))
            dq_report[table_name] = table_report(df, primary_key)
        else:
            dq_report[table_name] = {
                "error": f"Path not found: {str(path)}"
            }

    output_path = REPORTS_DIR / "dq_report.json"

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(dq_report, file, indent=4)

    print(f"DQ report written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    build_dq_report()
