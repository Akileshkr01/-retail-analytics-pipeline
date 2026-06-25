from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import os
import glob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("PowerBIExport")

GOLD_BASE_PATH   = "/opt/spark/data/gold"
EXPORT_BASE_PATH = "/opt/spark/data/powerbi"
APP_NAME         = "RetailAnalytics_PowerBIExport"
SPARK_MASTER     = "spark://spark-master:7077"

GOLD_TABLES = {
    "daily_revenue"        : f"{GOLD_BASE_PATH}/daily_revenue",
    "category_performance" : f"{GOLD_BASE_PATH}/category_performance",
    "city_revenue"         : f"{GOLD_BASE_PATH}/city_revenue",
    "payment_insights"     : f"{GOLD_BASE_PATH}/payment_insights",
}


def get_spark_session():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def export_table_to_csv(spark, table_name, parquet_path):
    logger.info(f"Exporting {table_name}...")

    df = spark.read.parquet(parquet_path)
    record_count = df.count()

    temp_path   = f"{EXPORT_BASE_PATH}/temp_{table_name}"
    final_path  = f"{EXPORT_BASE_PATH}/{table_name}.csv"

    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(temp_path)

    part_files = glob.glob(os.path.join(temp_path, "part-*.csv"))
    
    if part_files:
        part_file = part_files[0]
        if os.path.exists(final_path):
            os.remove(final_path)
            
        os.rename(part_file, final_path)
        
        for f in os.listdir(temp_path):
            os.remove(os.path.join(temp_path, f))
        os.rmdir(temp_path)
        
        logger.info(f"  Exported: {final_path} | {record_count:,} rows")
    else:
        logger.error(f"  Part file not found for {table_name}")

    return record_count


def print_csv_previews(spark):
    logger.info("CSV previews for verification:")

    for table_name in GOLD_TABLES:
        csv_path = f"{EXPORT_BASE_PATH}/{table_name}.csv"
        if not os.path.exists(csv_path):
            continue
            
        logger.info(f"Preview: {table_name}")

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)

        df.show(3, truncate=True)
        logger.info(f"  Columns: {df.columns}")


def run_export():
    logger.info("= = = =")
    logger.info("Starting Power BI Export")
    logger.info("= = = =")

    os.makedirs(EXPORT_BASE_PATH, exist_ok=True)
    spark = get_spark_session()

    export_summary = {}
    for table_name, parquet_path in GOLD_TABLES.items():
        try:
            count = export_table_to_csv(spark, table_name, parquet_path)
            export_summary[table_name] = count
        except Exception as e:
            logger.error(f"Failed to export {table_name}: {str(e)}")

    print_csv_previews(spark)

    logger.info("= = = =")
    logger.info("Power BI Export Summary")
    logger.info("= = = =")
    for table_name, count in export_summary.items():
        csv_path = f"{EXPORT_BASE_PATH}/{table_name}.csv"
        logger.info(f"  {table_name:<28}: {count:>6,} rows -> {csv_path}")

    logger.info("=" * 60)
    logger.info("All files ready for Power BI Desktop import.")
    spark.stop()


if __name__ == "__main__":
    run_export()