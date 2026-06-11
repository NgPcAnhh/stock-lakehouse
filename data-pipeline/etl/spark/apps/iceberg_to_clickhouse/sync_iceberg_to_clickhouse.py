from pyspark.sql import SparkSession

# 1. Khởi tạo Spark Session tích hợp Iceberg REST Catalog và ClickHouse
# - Cấu hình Spark Extensions cho Iceberg
# - Đăng ký catalog mang tên 'stock_catalog' sử dụng giao thức REST
# - Định nghĩa các thư viện cần nạp (Iceberg runtime + AWS SDK + ClickHouse connector)
spark = (SparkSession.builder \
    .appName("IcebergToClickHouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.stock_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.stock_catalog.type", "rest") \
    .config("spark.sql.catalog.stock_catalog.uri", "http://iceberg-catalog:8181") \
    .config("spark.sql.catalog.stock_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.stock_catalog.warehouse", "s3a://stock-datalake") \
    .config("spark.sql.catalog.stock_catalog.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.stock_catalog.s3.path-style-access", "true") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.clickhouse:clickhouse-spark-runtime-3.5_2.12:0.6.0") \
    .getOrCreate())

# 2. Đọc bảng chứng khoán từ Iceberg Catalog
# (Giả sử bạn đã có bảng 'stock_ticks' trong database/namespace 'raw')
df_ticks = spark.read.table("stock_catalog.raw.stock_ticks")

# Thực hiện xử lý biến đổi (ví dụ: filter, tính Moving Average...) tại đây
# df_processed = df_ticks.filter(...)

# 3. Ghi dữ liệu trực tiếp vào ClickHouse
# (Sử dụng clickhouse-spark-runtime connector đã nạp ở trên)
df_ticks.write \
    .format("clickhouse") \
    .option("clickhouse.host", "clickhouse") \
    .option("clickhouse.port", "8123") \
    .option("clickhouse.user", "default") \
    .option("clickhouse.password", "default") \
    .option("clickhouse.database", "default") \
    .option("clickhouse.table", "stock_prices") \
    .mode("append") \
    .save()
