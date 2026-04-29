.config("spark.sql.parquet.compression.codec", "uncompressed")
.config("spark.driver.host", "127.0.0.1")
.config("spark.driver.bindAddress", "127.0.0.1")
.config("spark.local.hostname", "localhost")

spark = (
    SparkSession.builder
    .appName("Bronze Ingestion")
    .master("local[2]")
    .config("spark.sql.parquet.compression.codec", "uncompressed")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.local.hostname", "localhost")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
