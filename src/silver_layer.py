from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("SilverLayer")

BRONZE_INPUT_PATH  = "/opt/spark/data/bronze/transactions"
SILVER_OUTPUT_PATH = "/opt/spark/data/silver/transactions"
APP_NAME           = "RetailAnalytics_SilverLayer"
SPARK_MASTER       = "spark://spark-master:7077"


def get_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark):
    logger.info(f"Reading Bronze Parquet from: {BRONZE_INPUT_PATH}")
    df = spark.read.parquet(BRONZE_INPUT_PATH)
    logger.info("--- BRONZE RAW SCHEMA ---")
    df.printSchema()
    logger.info(f"Bronze records loaded: {df.count():,}")
    return df


def trim_string_columns(df):
    string_cols = [
        "transaction_id", "customer_id", "category",
        "product_name", "city", "store_type", "payment_method"
    ]
    # Check which string columns actually exist in the incoming schema
    available_cols = [c for c in string_cols if c in df.columns]
    for col_name in available_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    logger.info(f"Trimmed whitespace from available string columns: {available_cols}")
    return df


def drop_non_recoverable_nulls(df, before_count):
    # Dynamic checks to avoid failing if columns are named differently
    for col_name in ["transaction_id", "price", "quantity", "transaction_date", "category"]:
        if col_name in df.columns:
            df = df.filter(F.col(col_name).isNotNull())
    
    after_count = df.count()
    logger.info(f"Null drop stage    : {before_count:,} -> {after_count:,} (dropped {before_count - after_count:,})")
    return df, after_count


def filter_invalid_values(df, before_count):
    if "price" in df.columns:
        df = df.filter(F.col("price") > 0)
    if "quantity" in df.columns:
        df = df.filter(F.col("quantity") > 0)

    after_count = df.count()
    logger.info(f"Invalid value stage: {before_count:,} -> {after_count:,} (dropped {before_count - after_count:,})")
    return df, after_count


def standardize_dates(df):
    if "transaction_date" not in df.columns:
        logger.error("CRITICAL: 'transaction_date' column missing from data framework!")
        return df

    # Adding alternative common formats to rule out string parsing drops
    df = df.withColumn(
        "transaction_date_std",
        F.coalesce(
            F.to_date(F.col("transaction_date"), "yyyy-MM-dd"),
            F.to_date(F.col("transaction_date"), "dd/MM/yyyy"),
            F.to_date(F.col("transaction_date"), "MM-dd-yyyy"),
            F.to_date(F.col("transaction_date"), "yyyy/MM/dd"),
            F.to_date(F.col("transaction_date"), "dd-MM-yyyy")
        )
    )

    before = df.count()
    df = df.filter(F.col("transaction_date_std").isNotNull())
    after = df.count()
    logger.info(f"Date parse stage   : {before:,} -> {after:,} (dropped {before - after:,} unparseable dates)")

    df = df.drop("transaction_date") \
           .withColumnRenamed("transaction_date_std", "transaction_date")

    return df


def standardize_category(df):
    if "category" in df.columns:
        df = df.withColumn(
            "category",
            F.initcap(F.lower(F.trim(F.col("category"))))
        )
        logger.info("Standardized category to title case.")
    return df


def standardize_store_type(df):
    if "store_type" in df.columns:
        df = df.withColumn(
            "store_type",
            F.when(
                F.lower(F.trim(F.col("store_type"))) == "online",
                F.lit("Online")
            ).when(
                F.lower(F.trim(F.col("store_type"))) == "offline",
                F.lit("Offline")
            ).otherwise(F.lit("Unknown"))
        )
        logger.info("Standardized store_type to Online / Offline / Unknown.")
    return df


def standardize_city(df):
    if "city" in df.columns:
        df = df.withColumn(
            "city",
            F.initcap(F.trim(F.col("city")))
        )
    return df


def standardize_product_name(df):
    if "product_name" in df.columns:
        df = df.withColumn(
            "product_name",
            F.initcap(F.trim(F.col("product_name")))
        )
    return df


def clamp_discount(df):
    if "discount" in df.columns:
        df = df.withColumn(
            "discount",
            F.when(F.col("discount") < 0.0, F.lit(0.0))
             .when(F.col("discount") > 0.99, F.lit(0.0))
             .otherwise(F.col("discount"))
        )
    return df


def fill_null_payment_method(df):
    if "payment_method" in df.columns:
        df = df.withColumn(
            "payment_method",
            F.when(F.col("payment_method").isNull(), F.lit("Unknown"))
             .otherwise(F.col("payment_method"))
        )
    return df


def fill_null_customer_id(df):
    if "customer_id" in df.columns:
        df = df.withColumn(
            "customer_id",
            F.when(F.col("customer_id").isNull(), F.lit("UNKNOWN"))
             .otherwise(F.col("customer_id"))
        )
    return df


def deduplicate(df, before_count):
    # Safely select sorting criteria based on available runtime schema
    order_col = "transaction_date" if "transaction_date" in df.columns else df.columns[0]
    window_spec = Window.partitionBy("transaction_id").orderBy(order_col)

    df = df.withColumn("row_num", F.row_number().over(window_spec))
    df = df.filter(F.col("row_num") == 1).drop("row_num")

    after_count = df.count()
    logger.info(f"Dedup stage        : {before_count:,} -> {after_count:,} (removed {before_count - after_count:,} duplicates)")
    return df, after_count


def compute_net_revenue(df):
    if "price" in df.columns and "quantity" in df.columns:
        discount_col = F.col("discount") if "discount" in df.columns else F.lit(0.0)
        df = df.withColumn(
            "net_revenue",
            F.round(F.col("price") * F.col("quantity") * (F.lit(1.0) - discount_col), 2)
        )
        logger.info("Computed net_revenue field.")
    return df


def add_silver_metadata(df):
    df = df.withColumn("silver_processed_at", F.current_timestamp()) \
           .withColumn("layer", F.lit("silver"))
    return df


def select_final_columns(df):
    # Dynamic select to match whatever columns are actually generated cleanly
    fallbacks = [
        "transaction_id", "customer_id", "transaction_date", "category",
        "product_name", "price", "quantity", "discount", "net_revenue",
        "city", "store_type", "payment_method", "source_file",
        "ingestion_timestamp", "silver_processed_at", "layer"
    ]
    target_cols = [c for c in fallbacks if c in df.columns]
    df = df.select(*target_cols)
    return df


def write_silver_parquet(df):
    logger.info(f"Writing Silver Parquet to: {SILVER_OUTPUT_PATH}")
    # Force data collection to ensure files write before validation blocks
    df.cache()
    
    if "category" in df.columns:
        df.write \
            .mode("overwrite") \
            .partitionBy("category") \
            .parquet(SILVER_OUTPUT_PATH)
    else:
        df.write \
            .mode("overwrite") \
            .parquet(SILVER_OUTPUT_PATH)
            
    logger.info("Silver Parquet write complete.")


def validate_silver_output(spark):
    logger.info("Validating Silver output...")

    # Flexible path parsing strategy to handle empty root nodes cleanly
    try:
        df = spark.read.parquet(SILVER_OUTPUT_PATH + "/*")
    except Exception:
        df = spark.read.parquet(SILVER_OUTPUT_PATH)

    total = df.count()
    logger.info(f"Silver record count validated: {total:,}")

    if total > 0 and "category" in df.columns:
        categories = [row["category"] for row in df.select("category").distinct().collect()]
        categories.sort()
        logger.info(f"Categories found    : {categories}")
        
        if "net_revenue" in df.columns:
            revenue_total = df.agg(F.round(F.sum("net_revenue"), 2).alias("total")).collect()[0]["total"]
            logger.info(f"Total net revenue   : {revenue_total if revenue_total else 0:,}")

    df.printSchema()
    df.show(5, truncate=True)


def run_silver_pipeline():
    logger.info("=" * 60)
    logger.info("Starting Debug Silver Layer Pipeline")
    logger.info("=" * 60)

    spark = get_spark_session()
    df = read_bronze(spark)
    initial_count = df.count()

    df = trim_string_columns(df)
    df = standardize_category(df)
    df = standardize_store_type(df)
    df = standardize_city(df)
    df = standardize_product_name(df)

    df, count_after_nulls = drop_non_recoverable_nulls(df, initial_count)
    df, count_after_invalid = filter_invalid_values(df, count_after_nulls)
    df = standardize_dates(df)
    count_after_dates = df.count()

    df = clamp_discount(df)
    df = fill_null_payment_method(df)
    df = fill_null_customer_id(df)

    df, count_after_dedup = deduplicate(df, count_after_dates)
    df = compute_net_revenue(df)
    df = add_silver_metadata(df)
    df = select_final_columns(df)

    write_silver_parquet(df)
    validate_silver_output(spark)

    logger.info("Silver Layer Pipeline Complete.")
    spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()