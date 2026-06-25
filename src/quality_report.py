from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("QualityReport")

BRONZE_PATH  = "/opt/spark/data/bronze/transactions"
SILVER_PATH  = "/opt/spark/data/silver/transactions"
GOLD_PATHS   = {
    "daily_revenue"        : "/opt/spark/data/gold/daily_revenue",
    "category_performance" : "/opt/spark/data/gold/category_performance",
    "city_revenue"         : "/opt/spark/data/gold/city_revenue",
    "payment_insights"     : "/opt/spark/data/gold/payment_insights",
}
SPARK_MASTER = "spark://spark-master:7077"


def get_spark_session():
    spark = SparkSession.builder \
        .appName("QualityReport") \
        .master(SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def report_bronze(spark):
    logger.info("BRONZE LAYER REPORT")
    logger.info("= = = =")

    df = spark.read.parquet(BRONZE_PATH)
    total = df.count()
    logger.info(f"Total records        : {total:,}")
    logger.info(f"Columns              : {len(df.columns)}")

    check_cols = [
        "transaction_id", "customer_id", "price",
        "quantity", "category", "transaction_date"
    ]
    for col_name in check_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()
        pct = (null_count / total) * 100
        logger.info(f"  Null [{col_name:<20}]: {null_count:>8,} ({pct:.2f}%)")

    partitions = [
        row["ingestion_year"]
        for row in df.select("ingestion_year").distinct().orderBy("ingestion_year").collect()
    ]
    logger.info(f"Partitions (year)    : {partitions}")

    return total


def report_silver(spark, bronze_count):
    logger.info("SILVER LAYER REPORT")
    logger.info("= = = =")

    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    retention = (total / bronze_count) * 100

    logger.info(f"Total records        : {total:,}")
    logger.info(f"Retention rate       : {retention:.2f}%")
    logger.info(f"Records cleaned out  : {bronze_count - total:,}")

    check_cols = [
        "transaction_id", "customer_id", "price",
        "quantity", "category", "transaction_date", "net_revenue"
    ]
    for col_name in check_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()
        logger.info(f"  Null [{col_name:<20}]: {null_count:>8,}")

    logger.info("Category distribution:")
    df.groupBy("category") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(truncate=False)

    logger.info("Store type distribution:")
    df.groupBy("store_type") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(truncate=False)

    revenue_stats = df.agg(
        F.round(F.sum("net_revenue"), 2).alias("total_net_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_net_revenue"),
        F.round(F.min("net_revenue"), 2).alias("min_net_revenue"),
        F.round(F.max("net_revenue"), 2).alias("max_net_revenue")
    ).collect()[0]

    logger.info(f"Total net revenue    : {revenue_stats['total_net_revenue']:,}")
    logger.info(f"Avg net revenue      : {revenue_stats['avg_net_revenue']:,}")
    logger.info(f"Min net revenue      : {revenue_stats['min_net_revenue']:,}")
    logger.info(f"Max net revenue      : {revenue_stats['max_net_revenue']:,}")

    partitions = [
        row["category"]
        for row in df.select("category").distinct().orderBy("category").collect()
    ]
    logger.info(f"Partitions (category): {partitions}")

    return total


def report_gold(spark):
    logger.info("GOLD LAYER REPORT")
    logger.info("= = = =")

    for table_name, path in GOLD_PATHS.items():
        df = spark.read.parquet(path)
        count = df.count()
        columns = df.columns
        logger.info(f"Table: {table_name}")
        logger.info(f"  Rows    : {count:,}")
        logger.info(f"  Columns : {columns}")

    logger.info("Top 3 revenue days:")
    spark.read.parquet(GOLD_PATHS["daily_revenue"]) \
        .orderBy(F.desc("total_revenue")) \
        .select("transaction_date", "total_revenue", "total_transactions") \
        .show(3, truncate=False)

    logger.info("Category revenue ranking:")
    spark.read.parquet(GOLD_PATHS["category_performance"]) \
        .select("revenue_rank", "category", "total_revenue", "revenue_share_pct") \
        .orderBy("revenue_rank") \
        .show(truncate=False)

    logger.info("Top 5 cities by revenue:")
    spark.read.parquet(GOLD_PATHS["city_revenue"]) \
        .select("revenue_rank", "city", "total_revenue", "revenue_share_pct") \
        .orderBy("revenue_rank") \
        .show(5, truncate=False)

    logger.info("Payment method breakdown:")
    spark.read.parquet(GOLD_PATHS["payment_insights"]) \
        .select("usage_rank", "payment_method", "total_transactions", "transaction_share_pct", "top_category") \
        .orderBy("usage_rank") \
        .show(truncate=False)


def run_quality_report():
    logger.info("= = = =")
    logger.info("Retail Analytics - Full Data Quality Report")
    logger.info("= = = =")

    spark = get_spark_session()

    bronze_count = report_bronze(spark)
    logger.info("")
    silver_count = report_silver(spark, bronze_count)
    logger.info("")
    report_gold(spark)

    logger.info("= = = =")
    logger.info("Pipeline Data Flow Summary")
    logger.info("= = = =")
    logger.info(f"Raw CSV (generated)  : ~1,000,000 records")
    logger.info(f"Bronze (ingested)    : {bronze_count:,} records")
    logger.info(f"Silver (cleaned)     : {silver_count:,} records")
    logger.info(f"Gold (aggregated)    : 4 BI-ready tables")
    logger.info(f"Power BI exports     : 4 CSV files")
    logger.info("= = = =")

    spark.stop()


if __name__ == "__main__":
    run_quality_report()