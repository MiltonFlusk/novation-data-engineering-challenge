from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date, sum as spark_sum, count, avg
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"

BASE_DIR = Path(__file__).resolve().parents[1]

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


def create_spark_session():
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.8"

    return (
        SparkSession.builder
        .appName("Gold Layer Build")
        .master("local[*]")
        .getOrCreate()
    )


def write_table(df, path):
    df.coalesce(1).write.mode("overwrite").parquet(str(path))


def build_gold_layer():
    print("Starting Gold layer build")

    spark = create_spark_session()

    customers_path = SILVER_DIR / "customers"
    accounts_path = SILVER_DIR / "accounts"
    transactions_path = SILVER_DIR / "transactions"

    customers = spark.read.parquet(str(customers_path))
    accounts = spark.read.parquet(str(accounts_path))
    transactions = spark.read.parquet(str(transactions_path))

    # -------------------------
    # Dimension: Customers
    # -------------------------
    dim_customers = customers.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone_number"),
        col("kyc_status"),
        col("risk_rating")
    ).dropDuplicates(["customer_id"])

    # -------------------------
    # Dimension: Accounts
    # -------------------------
    dim_accounts = accounts.select(
        col("account_id"),
        col("customer_id"),
        col("account_type"),
        col("account_status"),
        col("open_date")
    ).dropDuplicates(["account_id"])

    # -------------------------
    # Dimension: Date
    # -------------------------
    dim_date = transactions.select(
        to_date(col("transaction_timestamp")).alias("date")
    ).dropDuplicates()

    dim_date = dim_date.select(
        col("date"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day")
    )

    # -------------------------
    # Fact: Transactions
    # -------------------------
    fact_transactions = transactions.select(
        col("transaction_id"),
        col("account_id"),
        to_date(col("transaction_timestamp")).alias("date"),
        col("transaction_type"),
        col("amount"),
        col("currency"),
        col("merchant_category"),
        col("channel"),
        col("is_flagged")
    )

    # -------------------------
    # Optional Aggregated Fact
    # -------------------------
    fact_daily_account_summary = fact_transactions.groupBy(
        "account_id",
        "date"
    ).agg(
        count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_amount")
    )

    write_table(dim_customers, GOLD_DIR / "dim_customers")
    write_table(dim_accounts, GOLD_DIR / "dim_accounts")
    write_table(dim_date, GOLD_DIR / "dim_date")
    write_table(fact_transactions, GOLD_DIR / "fact_transactions")
    write_table(fact_daily_account_summary, GOLD_DIR /
                "fact_daily_account_summary")

    print("Gold layer build complete")

    spark.stop()


if __name__ == "__main__":
    build_gold_layer()
