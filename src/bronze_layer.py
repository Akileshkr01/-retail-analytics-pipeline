from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
from pyspark.sql import functions as F
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("BronzeLayer")

RAW_INPUT_PATH = "/opt/spark/data/raw/transactions.csv"
BRONZE_OUTPUT_PATH = "/opt/spark/data/bronze/transactions"
SOURCE_FILE_NAME = "transactions.csv"
LAYER_NAME = "bronze"
APP_NAME = "RetailAnalytics_BronzeLayer"
SPARK_MASTER = "spark://spark-master:7077"


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


def define_raw_schema():
    schema = StructType([
        StructField("transaction_id",   StringType(),  True),
        StructField("customer_id",      StringType(),  True),
        StructField("transaction_date", StringType(),  True),
        StructField("category",         StringType(),  True),
        StructField("product_name",     StringType(),  True),
        StructField("price",            DoubleType(),  True),
        StructField("quantity",         DoubleType(),  True),
        StructField("discount",         DoubleType(),  True),
        StructField("city",             StringType(),  True),
        StructField("store_type",       StringType(),  True),
        StructField("payment_method",   StringType(),  True),
    ])
    return schema


def read_raw_csv(spark, schema):
    logger.info(f"Reading raw CSV from: {RAW_INPUT_PATH}")

    df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("nullValue", "") \
        .option("emptyValue", "") \
        .schema(schema) \
        .csv(RAW_INPUT_PATH)

    return df


def add_metadata_columns(df):
    df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
           .withColumn("source_file", F.lit(SOURCE_FILE_NAME)) \
           .withColumn("layer", F.lit(LAYER_NAME)) \
           .withColumn(
               "ingestion_year",
               F.year(
                   F.coalesce(
                       F.to_date(F.col("transaction_date"), "yyyy-MM-dd"),
                       F.to_date(F.col("transaction_date"), "dd/MM/yyyy"),
                       F.to_date(F.col("transaction_date"), "MM-dd-yyyy")
                   )
               )
           )
    return df


def log_ingestion_statistics(df):
    logger.info("Computing ingestion statistics...")

    total_records = df.count()
    logger.info(f"Total records ingested : {total_records:,}")

    columns_to_check = [
        "transaction_id", "customer_id", "transaction_date",
        "category", "product_name", "price",
        "quantity", "discount", "city",
        "store_type", "payment_method"
    ]

    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in columns_to_check
    ]).collect()[0]

    logger.info("Null value counts per column:")
    for col_name in columns_to_check:
        count = null_counts[col_name]
        pct = (count / total_records) * 100 if total_records > 0 else 0
        logger.info(f"  {col_name:<25} : {count:>8,}  ({pct:.2f}%)")

    return total_records


def write_bronze_parquet(df):
    logger.info(f"Writing Bronze Parquet to: {BRONZE_OUTPUT_PATH}")

    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year") \
        .parquet(BRONZE_OUTPUT_PATH)

    logger.info("Bronze Parquet write complete.")


def validate_bronze_output(spark):
    logger.info("Validating Bronze output...")

    df_validate = spark.read.parquet(BRONZE_OUTPUT_PATH)
    record_count = df_validate.count()
    partitions = df_validate.select("ingestion_year").distinct().collect()
    partition_years = sorted([row["ingestion_year"] for row in partitions if row["ingestion_year"] is not None])

    logger.info(f"Bronze records written     : {record_count:,}")
    logger.info(f"Partitions (ingestion_year): {partition_years}")
    logger.info(f"Schema:")
    df_validate.printSchema()


def run_bronze_pipeline():
    logger.info("=" * 60)
    logger.info("Starting Bronze Layer Pipeline")
    logger.info("=" * 60)

    spark = get_spark_session()
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"App name     : {spark.sparkContext.appName}")

    schema = define_raw_schema()
    logger.info("Schema defined with explicit types (no inferSchema)")

    df_raw = read_raw_csv(spark, schema)

    df_bronze = add_metadata_columns(df_raw)

    total_records = log_ingestion_statistics(df_bronze)

    write_bronze_parquet(df_bronze)

    validate_bronze_output(spark)

    logger.info("= = = = = = =  =")
    logger.info("Bronze Layer Pipeline Complete")
    logger.info(f"Total records processed: {total_records:,}")
    logger.info(f"Output location        : {BRONZE_OUTPUT_PATH}")
    logger.info("= = = = = = = =")

    spark.stop()


if __name__ == "__main__":
    run_bronze_pipeline()