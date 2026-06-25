from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("connectivity-test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print(spark.version)
spark.stop()