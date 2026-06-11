from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestRead") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

try:
    df = spark.read.parquet("s3a://thongtin-congty-va-bctc/realtime/2026-06-10/")
    print("COUNT IS:", df.count())
    df.show(5)
except Exception as e:
    print("ERROR IS:", str(e))
