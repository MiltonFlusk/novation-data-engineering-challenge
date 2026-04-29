from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

BASE_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = BASE_DIR / "data" / "output" / "gold"

spark = (
    SparkSession.builder
    .appName("Check Gold Schema")
    .master("local[2]")
    .getOrCreate()
)

tables = {
    "dim_accounts": GOLD_DIR / "dim_accounts",
    "dim_customers": GOLD_DIR / "dim_customers",
    "fact_transactions": GOLD_DIR / "fact_transactions",
}

for name, path in tables.items():
    print("\n" + "=" * 80)
    print(name)
    print("=" * 80)

    df = spark.read.parquet(str(path))

    print("Row count:", df.count())
    print("Column count:", len(df.columns))
    print("Columns:")
    for c in df.columns:
        print(" -", c)

    print("\nSchema:")
    df.printSchema()

spark.stop()