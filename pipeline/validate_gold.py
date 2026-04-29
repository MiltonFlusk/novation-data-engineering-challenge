from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, round as spark_round


BASE_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = BASE_DIR / "data" / "output" / "gold"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("Validate Gold Layer")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = create_spark()

    fact = spark.read.format("delta").load(str(GOLD_DIR / "fact_transactions"))
    accounts = spark.read.format("delta").load(str(GOLD_DIR / "dim_accounts"))
    customers = spark.read.format("delta").load(str(GOLD_DIR / "dim_customers"))

    print("\n=== Query 1: Transaction Volume by Type ===")
    (
        fact.groupBy("transaction_type")
        .agg(
            count("*").alias("record_count"),
            spark_sum("amount").alias("total_amount")
        )
        .orderBy("transaction_type")
        .show(truncate=False)
    )

    print("\n=== Query 2: Zero Unlinked Accounts ===")
    unlinked_accounts = (
        accounts.alias("a")
        .join(customers.alias("c"), col("a.customer_id") == col("c.customer_id"), "left")
        .where(col("c.customer_id").isNull())
        .count()
    )

    print("unlinked_accounts:", unlinked_accounts)

    print("\n=== Query 3: Province Distribution ===")
    (
        accounts.alias("a")
        .join(customers.alias("c"), col("a.customer_id") == col("c.customer_id"), "inner")
        .groupBy("c.province")
        .agg(countDistinct("a.account_id").alias("account_count"))
        .orderBy("c.province")
        .show(20, truncate=False)
    )

    print("\n=== Key Null Checks ===")

    checks = {
        "dim_accounts.account_sk": accounts.where(col("account_sk").isNull()).count(),
        "dim_accounts.account_id": accounts.where(col("account_id").isNull()).count(),
        "dim_accounts.customer_id": accounts.where(col("customer_id").isNull()).count(),
        "dim_customers.customer_sk": customers.where(col("customer_sk").isNull()).count(),
        "dim_customers.customer_id": customers.where(col("customer_id").isNull()).count(),
        "fact_transactions.transaction_sk": fact.where(col("transaction_sk").isNull()).count(),
        "fact_transactions.transaction_id": fact.where(col("transaction_id").isNull()).count(),
        "fact_transactions.account_sk": fact.where(col("account_sk").isNull()).count(),
        "fact_transactions.customer_sk": fact.where(col("customer_sk").isNull()).count(),
        "fact_transactions.currency_not_zar": fact.where(col("currency") != "ZAR").count(),
    }

    for name, value in checks.items():
        print(name, value)

    spark.stop()


if __name__ == "__main__":
    main()