from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("GoldLayer")

SILVER_INPUT_PATH       = "/opt/spark/data/silver/transactions"
GOLD_BASE_PATH          = "/opt/spark/data/gold"
GOLD_DAILY_REVENUE      = f"{GOLD_BASE_PATH}/daily_revenue"
GOLD_CATEGORY_PERF      = f"{GOLD_BASE_PATH}/category_performance"
GOLD_CITY_REVENUE       = f"{GOLD_BASE_PATH}/city_revenue"
GOLD_PAYMENT_INSIGHTS   = f"{GOLD_BASE_PATH}/payment_insights"
APP_NAME                = "RetailAnalytics_GoldLayer"
SPARK_MASTER            = "spark://spark-master:7077"


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


def read_silver(spark):
    logger.info(f"Reading Silver Parquet from: {SILVER_INPUT_PATH}")
    df = spark.read.parquet(SILVER_INPUT_PATH)
    count = df.count()
    logger.info(f"Silver records loaded: {count:,}")
    return df


def build_daily_revenue(df):
    logger.info("Building gold_daily_revenue...")

    daily = df.groupBy("transaction_date").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_transaction_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("gross_revenue"),
        F.round(F.avg("discount"), 4).alias("avg_discount_rate"),
        F.sum("quantity").alias("total_units_sold")
    )

    window_date = Window.orderBy("transaction_date")

    daily = daily.withColumn(
        "revenue_7day_rolling_avg",
        F.round(
            F.avg("total_revenue").over(
                window_date.rowsBetween(-6, 0)
            ),
            2
        )
    )

    daily = daily.withColumn(
        "revenue_prev_day",
        F.lag("total_revenue", 1).over(window_date)
    )

    daily = daily.withColumn(
        "revenue_day_over_day_pct",
        F.round(
            F.when(
                F.col("revenue_prev_day").isNotNull() & (F.col("revenue_prev_day") > 0),
                ((F.col("total_revenue") - F.col("revenue_prev_day")) / F.col("revenue_prev_day")) * 100
            ).otherwise(F.lit(None)),
            2
        )
    )

    daily = daily.withColumn("year",  F.year("transaction_date")) \
                 .withColumn("month", F.month("transaction_date")) \
                 .withColumn("day_of_week", F.dayofweek("transaction_date")) \
                 .withColumn("week_of_year", F.weekofyear("transaction_date"))

    daily = daily.drop("revenue_prev_day")
    daily = daily.orderBy("transaction_date")

    record_count = daily.count()
    logger.info(f"gold_daily_revenue rows: {record_count:,}")
    return daily


def build_category_performance(df):
    logger.info("Building gold_category_performance...")

    category = df.groupBy("category").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_transaction_value"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("discount"), 4).alias("avg_discount_rate"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_name").alias("unique_products"),
        F.round(F.min("net_revenue"), 2).alias("min_transaction_value"),
        F.round(F.max("net_revenue"), 2).alias("max_transaction_value")
    )

    window_global = Window.partitionBy()
    category = category.withColumn(
        "revenue_share_pct",
        F.round((F.col("total_revenue") / F.sum("total_revenue").over(window_global)) * 100, 2)
    )

    window_rank = Window.orderBy(F.desc("total_revenue"))
    category = category.withColumn("revenue_rank", F.rank().over(window_rank))

    category = category.withColumn(
        "revenue_per_unit",
        F.round(F.col("total_revenue") / F.col("total_units_sold"), 2)
    )

    category = category.withColumn(
        "avg_basket_size",
        F.round(F.col("total_units_sold") / F.col("total_transactions"), 2)
    )

    category = category.orderBy("revenue_rank")

    record_count = category.count()
    logger.info(f"gold_category_performance rows: {record_count:,}")
    return category


def build_city_revenue(df):
    logger.info("Building gold_city_revenue...")

    city = df.groupBy("city").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_transaction_value"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("discount"), 4).alias("avg_discount_rate")
    )

    window_global = Window.partitionBy()
    city = city.withColumn(
        "revenue_share_pct",
        F.round((F.col("total_revenue") / F.sum("total_revenue").over(window_global)) * 100, 2)
    )

    window_rank = Window.orderBy(F.desc("total_revenue"))
    city = city.withColumn("revenue_rank", F.rank().over(window_rank))

    city = city.withColumn(
        "revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
    )

    online_offline = df.groupBy("city", "store_type").agg(
        F.round(F.sum("net_revenue"), 2).alias("channel_revenue")
    )

    online = online_offline.filter(F.col("store_type") == "Online") \
        .select(
            F.col("city").alias("city_join"),
            F.col("channel_revenue").alias("online_revenue")
        )

    offline = online_offline.filter(F.col("store_type") == "Offline") \
        .select(
            F.col("city").alias("city_join"),
            F.col("channel_revenue").alias("offline_revenue")
        )

    city = city.join(online,  city["city"] == online["city_join"],  "left").drop("city_join")
    city = city.join(offline, city["city"] == offline["city_join"], "left").drop("city_join")

    city = city.withColumn(
        "online_revenue",
        F.when(F.col("online_revenue").isNull(), F.lit(0.0)).otherwise(F.col("online_revenue"))
    ).withColumn(
        "offline_revenue",
        F.when(F.col("offline_revenue").isNull(), F.lit(0.0)).otherwise(F.col("offline_revenue"))
    )

    city = city.orderBy("revenue_rank")

    record_count = city.count()
    logger.info(f"gold_city_revenue rows: {record_count:,}")
    return city


def build_payment_insights(df):
    logger.info("Building gold_payment_insights...")

    payment = df.groupBy("payment_method").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_transaction_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("discount"), 4).alias("avg_discount_rate"),
        F.sum("quantity").alias("total_units_sold")
    )

    window_global = Window.partitionBy()
    payment = payment.withColumn(
        "transaction_share_pct",
        F.round((F.col("total_transactions") / F.sum("total_transactions").over(window_global)) * 100, 2)
    ).withColumn(
        "revenue_share_pct",
        F.round((F.col("total_revenue") / F.sum("total_revenue").over(window_global)) * 100, 2)
    )

    window_rank = Window.orderBy(F.desc("total_transactions"))
    payment = payment.withColumn("usage_rank", F.rank().over(window_rank))

    category_payment = df.groupBy("payment_method", "category").agg(
        F.count("transaction_id").alias("txn_count")
    )

    window_cat = Window.partitionBy("payment_method").orderBy(F.desc("txn_count"))
    top_category = category_payment.withColumn("cat_rank", F.row_number().over(window_cat)) \
        .filter(F.col("cat_rank") == 1) \
        .select(
            F.col("payment_method").alias("pm_join"),
            F.col("category").alias("top_category")
        )

    payment = payment.join(
        top_category,
        payment["payment_method"] == top_category["pm_join"],
        "left"
    ).drop("pm_join")

    payment = payment.orderBy("usage_rank")

    record_count = payment.count()
    logger.info(f"gold_payment_insights rows: {record_count:,}")
    return payment


def write_gold_table(df, path, table_name):
    logger.info(f"Writing {table_name} to: {path}")
    df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(path)
    logger.info(f"{table_name} write complete.")


def validate_gold_outputs(spark):
    logger.info("=" * 60)
    logger.info("Gold Layer Validation")
    logger.info("=" * 60)

    tables = {
        "daily_revenue"        : GOLD_DAILY_REVENUE,
        "category_performance" : GOLD_CATEGORY_PERF,
        "city_revenue"         : GOLD_CITY_REVENUE,
        "payment_insights"     : GOLD_PAYMENT_INSIGHTS,
    }

    for table_name, path in tables.items():
        df = spark.read.parquet(path)
        count = df.count()
        columns = df.columns
        logger.info(f"{table_name:<25}: {count:>6,} rows | columns: {columns}")

    logger.info("=" * 60)

    logger.info("Sample: gold_daily_revenue (first 5 rows)")
    spark.read.parquet(GOLD_DAILY_REVENUE).show(5, truncate=False)

    logger.info("Sample: gold_category_performance")
    spark.read.parquet(GOLD_CATEGORY_PERF).show(truncate=False)

    logger.info("Sample: gold_city_revenue (top 5 cities)")
    spark.read.parquet(GOLD_CITY_REVENUE).show(5, truncate=False)

    logger.info("Sample: gold_payment_insights")
    spark.read.parquet(GOLD_PAYMENT_INSIGHTS).show(truncate=False)


def run_gold_pipeline():
    logger.info("=" * 60)
    logger.info("Starting Gold Layer Pipeline")
    logger.info("=" * 60)

    spark = get_spark_session()

    df_silver = read_silver(spark)
    df_silver.cache()
    logger.info("Silver DataFrame cached for Gold aggregations.")

    df_daily    = build_daily_revenue(df_silver)
    df_category = build_category_performance(df_silver)
    df_city     = build_city_revenue(df_silver)
    df_payment  = build_payment_insights(df_silver)

    write_gold_table(df_daily,    GOLD_DAILY_REVENUE,    "gold_daily_revenue")
    write_gold_table(df_category, GOLD_CATEGORY_PERF,    "gold_category_performance")
    write_gold_table(df_city,     GOLD_CITY_REVENUE,     "gold_city_revenue")
    write_gold_table(df_payment,  GOLD_PAYMENT_INSIGHTS, "gold_payment_insights")

    df_silver.unpersist()
    logger.info("Silver cache released.")

    validate_gold_outputs(spark)

    logger.info("Gold Layer Pipeline Complete.")
    spark.stop()


if __name__ == "__main__":
    run_gold_pipeline()