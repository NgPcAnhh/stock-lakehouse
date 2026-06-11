import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Schema of the JSON payload from Kafka (flat structure)
json_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("last_volume", LongType(), True),
    StructField("total_volume", LongType(), True),
    StructField("total_value", DoubleType(), True),
    StructField("foreign_buy_qty", LongType(), True),
    StructField("foreign_sell_qty", LongType(), True),
    StructField("foreign_buy_val", DoubleType(), True),
    StructField("foreign_sell_val", DoubleType(), True),
    StructField("bid1_price", DoubleType(), True),
    StructField("bid1_qty", LongType(), True),
    StructField("bid2_price", DoubleType(), True),
    StructField("bid2_qty", LongType(), True),
    StructField("bid3_price", DoubleType(), True),
    StructField("bid3_qty", LongType(), True),
    StructField("ask1_price", DoubleType(), True),
    StructField("ask1_qty", LongType(), True),
    StructField("ask2_price", DoubleType(), True),
    StructField("ask2_qty", LongType(), True),
    StructField("ask3_price", DoubleType(), True),
    StructField("ask3_qty", LongType(), True),
    StructField("ref_price", DoubleType(), True),
    StructField("ceil_price", DoubleType(), True),
    StructField("floor_price", DoubleType(), True),
    StructField("change_percent", DoubleType(), True),
    StructField("change_value", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True)
])

# Initialize Spark Session (Configurations are loaded automatically from spark-defaults.conf)
spark = SparkSession.builder \
    .appName("Kafka-to-Iceberg-Streaming") \
    .getOrCreate()

# Read from Kafka topic 'market.quotes.raw'
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "market.quotes.raw") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON values and structure schema
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), json_schema).alias("data")) \
    .select("data.*")

# Cast columns to exact schema types and order them correctly to align with Iceberg
transformed_df = parsed_df \
    .withColumnRenamed("symbol", "ticker") \
    .select(
        col("ticker").cast(StringType()),
        to_timestamp(col("ts") / 1000).cast("timestamp").alias("prd_id"),
        col("last_price").cast("decimal(18, 4)"),
        col("avg_price").cast("decimal(18, 4)"),
        col("last_volume").cast("long"),
        col("total_volume").cast("long"),
        col("total_value").cast("decimal(18, 4)"),
        col("foreign_buy_qty").cast("long"),
        col("foreign_sell_qty").cast("long"),
        col("foreign_buy_val").cast("decimal(18, 4)"),
        col("foreign_sell_val").cast("decimal(18, 4)"),
        col("bid1_price").cast("decimal(18, 4)"),
        col("bid1_qty").cast("long"),
        col("bid2_price").cast("decimal(18, 4)"),
        col("bid2_qty").cast("long"),
        col("bid3_price").cast("decimal(18, 4)"),
        col("bid3_qty").cast("long"),
        col("ask1_price").cast("decimal(18, 4)"),
        col("ask1_qty").cast("long"),
        col("ask2_price").cast("decimal(18, 4)"),
        col("ask2_qty").cast("long"),
        col("ask3_price").cast("decimal(18, 4)"),
        col("ask3_qty").cast("long"),
        col("ref_price").cast("decimal(18, 4)"),
        col("ceil_price").cast("decimal(18, 4)"),
        col("floor_price").cast("decimal(18, 4)"),
        col("change_percent").cast("decimal(18, 4)"),
        col("change_value").cast("decimal(18, 4)"),
        col("high_price").cast("decimal(18, 4)"),
        col("low_price").cast("decimal(18, 4)"),
        current_timestamp().cast("timestamp").alias("import_time")
    )

# Write to Iceberg table stock_catalog.stock_db.fact_realtime_quotes
query = transformed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "s3a://stock-datalake/checkpoints/kafka_to_iceberg_v2/") \
    .toTable("stock_catalog.stock_db.fact_realtime_quotes")

print("🚀 Real-time Kafka to Iceberg Streaming has started. Awaiting termination...")
query.awaitTermination()

