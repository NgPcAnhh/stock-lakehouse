import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

def main():
    parser = argparse.ArgumentParser(description="Incremental sync from MinIO Landing raw CSV files to Iceberg tables")
    parser.add_argument("--source", required=True, help="Đường dẫn thư mục nguồn S3 (e.g., s3a://thongtin-congty-va-bctc/daily_price/date=2026-06-10/)")
    parser.add_argument("--target", required=True, help="Tên bảng Iceberg đích (e.g., fact_history_price)")
    parser.add_argument("--time_col", help="Tên cột thời gian gốc cần đổi tên sang prd_id")
    parser.add_argument("--time_type", choices=["date", "timestamp"], help="Kiểu dữ liệu thời gian: date hoặc timestamp")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Chế độ ghi đè hoặc nối tiếp: append hoặc overwrite")
    parser.add_argument("--static", help="Các cột tĩnh dạng key=val phân tách bằng dấu phẩy (e.g., asset_type=USD_VND)")
    parser.add_argument("--format", default="csv", choices=["csv", "parquet"], help="Định dạng file nguồn: csv hoặc parquet")
    
    args = parser.parse_args()
    
    print(f"\n🚀 Starting incremental sync:")
    print(f"   Source: {args.source}")
    print(f"   Format: {args.format}")
    print(f"   Target: {args.target}")
    print(f"   Time Column: {args.time_col} ({args.time_type})")
    print(f"   Write Mode: {args.mode}")
    if args.static:
        print(f"   Static values: {args.static}")
    
    # 1. Khởi tạo Spark Session (Cấu hình tự động nạp từ spark-defaults.conf)
    spark = SparkSession.builder \
        .appName(f"Sync-{args.target}") \
        .getOrCreate()
        
    try:
        # 2. Đọc file từ nguồn
        print(f"\n[1/3] Reading {args.format} files from {args.source}...")
        if args.format == "csv":
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(args.source)
        elif args.format == "parquet":
            df = spark.read.format("parquet") \
                .load(args.source)
            
        if df.count() == 0:
            print("⚠️ Source directory is empty. Exiting.")
            sys.exit(0)
            
        # 3. Chuẩn hóa tên cột thô thành chữ thường
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.strip().lower())
            
        # 3.5 Áp dụng các giá trị cột tĩnh (static values)
        from pyspark.sql.functions import lit
        if args.static:
            pairs = args.static.split(",")
            for pair in pairs:
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    k_clean = k.strip().lower()
                    v_clean = v.strip()
                    df = df.withColumn(k_clean, lit(v_clean))
            
        # 4. Áp dụng chuyển đổi thời gian sang prd_id
        if args.time_col:
            orig_col = args.time_col.strip().lower()
            if orig_col in df.columns:
                print(f"[2/3] Renaming and casting {orig_col} -> prd_id...")
                if args.time_type == "date":
                    df = df.withColumn("prd_id", to_date(col(orig_col)))
                elif args.time_type == "timestamp":
                    df = df.withColumn("prd_id", to_timestamp(col(orig_col)))
                
                # Xóa cột cũ để đồng bộ
                if orig_col != "prd_id":
                    df = df.drop(orig_col)
            else:
                print(f"⚠️ Warning: Time column '{orig_col}' not found in source schema. Available columns: {df.columns}")
        if "prd_id" in df.columns:
            df = df.sortWithinPartitions("prd_id")
        
        # 5. Khớp cấu hình schema với bảng Iceberg đích
        iceberg_table_path = f"stock_catalog.stock_db.{args.target}"
        target_df = spark.read.table(iceberg_table_path)
        target_schema = target_df.schema
        
        # Rename 'symbol' to 'ticker' if needed
        if "symbol" in df.columns and "ticker" not in df.columns and "ticker" in [f.name for f in target_schema]:
            print("[Info] Renaming 'symbol' -> 'ticker' to match target schema...")
            df = df.withColumnRenamed("symbol", "ticker")
        
        # Thêm các cột thiếu từ schema đích
        from pyspark.sql.functions import current_timestamp, lit
        for field in target_schema:
            if field.name not in df.columns:
                if field.name in ["import_time", "created_at", "inserted_at"]:
                    df = df.withColumn(field.name, current_timestamp())
                else:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
        
        # Chọn lại đúng thứ tự các cột như bảng đích
        df = df.select(*target_df.columns)
        
        # 5.5 Idempotency check: Delete existing records in target table to prevent duplication
        if args.mode == "append":
            try:
                if args.target == "fact_financial_reports" and "ticker" in df.columns and "year" in df.columns and "quarter" in df.columns:
                    print(f"[Info] Performing idempotency cleanup for BCTC...")
                    combos = df.select("ticker", "year", "quarter").distinct().collect()
                    for r in combos:
                        t, y, q = r["ticker"], r["year"], r["quarter"]
                        if t and y is not None and q:
                            spark.sql(f"DELETE FROM {iceberg_table_path} WHERE ticker = '{t}' AND year = {y} AND quarter = '{q}'")
                elif args.target == "fact_financial_ratios" and "ticker" in df.columns and "year" in df.columns and "quarter" in df.columns:
                    print(f"[Info] Performing idempotency cleanup for financial ratios...")
                    combos = df.select("ticker", "year", "quarter").distinct().collect()
                    for r in combos:
                        t, y, q = r["ticker"], r["year"], r["quarter"]
                        if t and y is not None and q is not None:
                            spark.sql(f"DELETE FROM {iceberg_table_path} WHERE ticker = '{t}' AND year = {y} AND quarter = {q}")
                elif "prd_id" in df.columns:
                    print(f"[Info] Performing idempotency cleanup on prd_id...")
                    unique_periods = df.select("prd_id").distinct().collect()
                    periods = [r["prd_id"] for r in unique_periods if r["prd_id"] is not None]
                    if periods:
                        if args.time_type == "date":
                            period_strs = []
                            for p in periods:
                                val_str = p.strftime('%Y-%m-%d') if hasattr(p, 'strftime') else str(p)
                                period_strs.append(f"DATE '{val_str}'")
                            filter_cond = f"prd_id IN ({', '.join(period_strs)})"
                        else:
                            # Optimize if there are too many timestamps (e.g. realtime quotes)
                            if len(periods) > 100:
                                from pyspark.sql.functions import min as spark_min, max as spark_max
                                min_max = df.select(spark_min("prd_id"), spark_max("prd_id")).collect()[0]
                                min_val = min_max[0].strftime('%Y-%m-%d %H:%M:%S') if hasattr(min_max[0], 'strftime') else str(min_max[0])
                                max_val = min_max[1].strftime('%Y-%m-%d %H:%M:%S') if hasattr(min_max[1], 'strftime') else str(min_max[1])
                                filter_cond = f"prd_id >= TIMESTAMP '{min_val}' AND prd_id <= TIMESTAMP '{max_val}'"
                            else:
                                period_strs = []
                                for p in periods:
                                    val_str = p.strftime('%Y-%m-%d %H:%M:%S') if hasattr(p, 'strftime') else str(p)
                                    period_strs.append(f"TIMESTAMP '{val_str}'")
                                filter_cond = f"prd_id IN ({', '.join(period_strs)})"
                        
                        delete_query = f"DELETE FROM {iceberg_table_path} WHERE {filter_cond}"
                        print(f"[Info] Executing: {delete_query}")
                        spark.sql(delete_query)
            except Exception as e_del:
                print(f"⚠️ Warning during idempotency cleanup: {str(e_del)}. Proceeding to write...")
        
        print(f"[3/3] Writing ({args.mode}) {df.count()} rows to Iceberg table {iceberg_table_path}...")
        
        # Ghi vào bảng Iceberg
        df.write \
            .format("iceberg") \
            .mode(args.mode) \
            .save(iceberg_table_path)
            
        print(f"✅ Incremental sync completed successfully for {args.target}!")
        
    except Exception as e:
        print(f"❌ Error during sync: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
