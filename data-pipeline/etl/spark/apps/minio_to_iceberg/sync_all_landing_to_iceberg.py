import argparse
import base64
import json
import sys
import unicodedata
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp

def slugify(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    ascii_text = re.sub(r"\(.*?\)", "", ascii_text)
    ascii_text = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_text)
    parts = ascii_text.strip().lower().split()
    slug = "_".join(parts)
    return slug.strip("_") or "unknown"

def list_s3_files(spark, s3_path):
    try:
        sc = spark.sparkContext
        jvm = sc._gateway.jvm
        Path = jvm.org.apache.hadoop.fs.Path
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem
        
        path = Path(s3_path)
        fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
        
        if fs.exists(path):
            statuses = fs.listStatus(path)
            files = []
            for status in statuses:
                if not status.isDirectory():
                    files.append(status.getPath().toString())
            return files
        return []
    except Exception as e:
        print(f"⚠️ Warning listing S3 files: {str(e)}")
        return []

def find_latest_metadata_file(spark, table_name):
    try:
        s3_path = f"s3a://stock-datalake/stock_db/{table_name}/metadata"
        files = list_s3_files(spark, s3_path)
        metadata_files = [f for f in files if f.endswith(".metadata.json")]
        if not metadata_files:
            return None
        metadata_files.sort()
        return metadata_files[-1]
    except Exception as e:
        print(f"⚠️ Error finding latest metadata file for {table_name}: {str(e)}")
        return None

def ensure_table_registered(spark, table_name):
    # Registration is unsupported by the REST catalog image, so we just log the status.
    # The table will be automatically created on write if it does not exist.
    full_table_name = f"stock_catalog.stock_db.{table_name}"
    try:
        if not spark.catalog.tableExists(full_table_name):
            print(f"🕵️ Table {full_table_name} does not exist in REST Catalog. It will be created on write.")
    except Exception as e:
        print(f"⚠️ Error checking table {full_table_name}: {str(e)}")

def sync_standard_table(spark, task):
    source = task["source"]
    target = task["target"]
    mode = task.get("mode", "append")
    file_format = task.get("format", "csv")
    time_col = task.get("time_col")
    time_type = task.get("time_type")
    static = task.get("static")

    print(f"\n⚡ Syncing table {target} from {source}...")
    
    # List S3 objects for logging
    s3_files = list_s3_files(spark, source)
    if s3_files:
        print(f"📂 Found {len(s3_files)} MinIO object(s) in partition directory:")
        for sf in s3_files:
            print(f"  - {sf}")
    else:
        print("📂 No direct files listed (or directory contains subdirectories/empty).")

    # 1. Read files
    if file_format == "csv":
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(source)
    elif file_format == "parquet":
        df = spark.read.format("parquet") \
            .load(source)
    else:
        raise ValueError(f"Unsupported format: {file_format}")
        
    row_count = df.count()
    if row_count == 0:
        print(f"⚠️ Source directory {source} is empty. Skipping.")
        return f"⚠️ Empty: {target} ({source})"
        
    # 2. Lowercase column names
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip().lower())
        
    # 3. Apply static values
    if static:
        pairs = static.split(",")
        for pair in pairs:
            if "=" in pair:
                k, v = pair.split("=", 1)
                k_clean = k.strip().lower()
                v_clean = v.strip()
                df = df.withColumn(k_clean, lit(v_clean))
                
    # 4. Handle time column casting
    if time_col:
        orig_col = time_col.strip().lower()
        if orig_col in df.columns:
            if time_type == "date":
                df = df.withColumn("prd_id", to_date(col(orig_col)))
            elif time_type == "timestamp":
                df = df.withColumn("prd_id", to_timestamp(col(orig_col)))
            
            if orig_col != "prd_id":
                df = df.drop(orig_col)
        else:
            print(f"⚠️ Time column '{orig_col}' not found in source schema. Available columns: {df.columns}")
            
    if "prd_id" in df.columns:
        df = df.sortWithinPartitions("prd_id")
        
    # 5. Schema alignment
    iceberg_table_path = f"stock_catalog.stock_db.{target}"
    ensure_table_registered(spark, target)
    
    if spark.catalog.tableExists(iceberg_table_path):
        target_df = spark.read.table(iceberg_table_path)
        target_schema = target_df.schema
        
        if "symbol" in df.columns and "ticker" not in df.columns and "ticker" in [f.name for f in target_schema]:
            df = df.withColumnRenamed("symbol", "ticker")
            
        for field in target_schema:
            if field.name not in df.columns:
                if field.name in ["import_time", "created_at", "inserted_at"]:
                    df = df.withColumn(field.name, current_timestamp())
                else:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
                    
        df = df.select(*target_df.columns)
    else:
        print(f"🕵️ Target table {iceberg_table_path} does not exist in catalog. Skipping schema alignment and preparing direct write schema...")
        if "symbol" in df.columns:
            df = df.withColumnRenamed("symbol", "ticker")
        
        for audit_col in ["import_time", "created_at", "inserted_at"]:
            if audit_col not in df.columns:
                df = df.withColumn(audit_col, current_timestamp())
    
    # 6. Idempotency cleanup for append mode
    if mode == "append":
        try:
            if target == "fact_financial_reports" and "ticker" in df.columns and "year" in df.columns and "quarter" in df.columns:
                print(f"[Info] Idempotency cleanup for BCTC...")
                combos = df.select("ticker", "year", "quarter").distinct().collect()
                for r in combos:
                    t, y, q = r["ticker"], r["year"], r["quarter"]
                    if t and y is not None and q:
                        spark.sql(f"DELETE FROM {iceberg_table_path} WHERE ticker = '{t}' AND year = {y} AND quarter = '{q}'")
            elif target == "fact_financial_ratios" and "ticker" in df.columns and "year" in df.columns and "quarter" in df.columns:
                print(f"[Info] Idempotency cleanup for financial ratios...")
                combos = df.select("ticker", "year", "quarter").distinct().collect()
                for r in combos:
                    t, y, q = r["ticker"], r["year"], r["quarter"]
                    if t and y is not None and q is not None:
                        spark.sql(f"DELETE FROM {iceberg_table_path} WHERE ticker = '{t}' AND year = {y} AND quarter = {q}")
            elif "prd_id" in df.columns:
                print(f"[Info] Idempotency cleanup on prd_id...")
                unique_periods = df.select("prd_id").distinct().collect()
                periods = [r["prd_id"] for r in unique_periods if r["prd_id"] is not None]
                if periods:
                    if time_type == "date":
                        period_strs = []
                        for p in periods:
                            val_str = p.strftime('%Y-%m-%d') if hasattr(p, 'strftime') else str(p)
                            period_strs.append(f"DATE '{val_str}'")
                        filter_cond = f"prd_id IN ({', '.join(period_strs)})"
                    else:
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
            
    # 7. Write to Iceberg
    print(f"Writing {row_count} rows ({mode}) to {iceberg_table_path}...")
    df.write \
        .format("iceberg") \
        .mode(mode) \
        .saveAsTable(iceberg_table_path)
        
    print(f"✅ Synced {target} successfully!")
    return f"✅ Success: {target} ({source})"

def sync_vn_macro_yearly(spark, task):
    source = task["source"]
    target = "fact_vn_macro_yearly"
    print(f"\n⚡ Syncing pivoted VN macro yearly from {source}...")
    
    # List S3 objects for logging
    s3_files = list_s3_files(spark, source)
    if s3_files:
        print(f"📂 Found {len(s3_files)} MinIO object(s) in partition directory:")
        for sf in s3_files:
            print(f"  - {sf}")
            
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source)
        
    if df.count() == 0:
        print("⚠️ Source is empty.")
        return f"⚠️ Empty: {target} ({source})"
        
    df = df.select([col(c).alias(c.strip()) for c in df.columns])
    
    data = df.collect()
    slug_map = {}
    for row in data:
        indicator = row['chi_so']
        if indicator:
            slug_map[indicator] = slugify(indicator)
            
    year_cols = [c for c in df.columns if c.startswith("YR")]
    pdf = df.toPandas()
    pdf.columns = pdf.columns.str.strip()
    keep_cols = ["chi_so"] + year_cols
    pdf = pdf[keep_cols].copy()
    
    pdf['chi_so'] = pdf['chi_so'].map(slug_map)
    pdf = pdf.set_index("chi_so")
    pdf_t = pdf.T
    pdf_t.index = pdf_t.index.str.replace("YR", "").astype(int)
    pdf_t.index.name = "year"
    pdf_t = pdf_t.reset_index()
    
    for c in pdf_t.columns:
        if c != "year":
            pdf_t[c] = pd.to_numeric(pdf_t[c], errors="coerce")
            
    spark_df = spark.createDataFrame(pdf_t)
    
    target_table_path = "stock_catalog.stock_db.fact_vn_macro_yearly"
    ensure_table_registered(spark, "fact_vn_macro_yearly")
    
    if spark.catalog.tableExists(target_table_path):
        target_df = spark.read.table(target_table_path)
        for field in target_df.schema:
            if field.name not in spark_df.columns:
                spark_df = spark_df.withColumn(field.name, lit(None).cast(field.dataType))
        spark_df = spark_df.select(*target_df.columns)
    else:
        print(f"🕵️ Target table {target_table_path} does not exist in catalog. Skipping schema alignment and preparing direct write schema...")
        for audit_col in ["import_time", "created_at", "inserted_at"]:
            if audit_col not in spark_df.columns:
                spark_df = spark_df.withColumn(audit_col, current_timestamp())
                
    print(f"Writing pivoted data to {target_table_path}...")
    spark_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(target_table_path)
        
    print(f"✅ Synced {target} successfully!")
    return f"✅ Success: {target} ({source})"

def main():
    parser = argparse.ArgumentParser(description="Sync multiple Landing prefixes to Iceberg tables in a single Spark Session")
    parser.add_argument("--base64_config", required=True, help="Base64 encoded JSON config containing lists of tasks")
    args = parser.parse_args()
    
    # Decode configuration
    try:
        decoded_bytes = base64.b64decode(args.base64_config)
        decoded_str = decoded_bytes.decode('utf-8')
        config = json.loads(decoded_str)
    except Exception as e:
        print(f"❌ Failed to decode base64 config: {str(e)}")
        sys.exit(1)
        
    tasks = config.get("tasks", [])
    if not tasks:
        print("⚠️ No tasks provided in config. Exiting.")
        sys.exit(0)
        
    print(f"🚀 Found {len(tasks)} tasks to sync in this Spark application.")
    
    spark = SparkSession.builder \
        .appName("Sync-All-Landing-To-Iceberg") \
        .getOrCreate()
        
    results = []
    has_failed = False
    
    for idx, task in enumerate(tasks, 1):
        target = task.get("target")
        source = task.get("source")
        script_type = task.get("script_type", "standard")
        
        print(f"\n--- Task [{idx}/{len(tasks)}]: {target} ({script_type}) ---")
        try:
            if script_type == "vn_macro_yearly":
                res = sync_vn_macro_yearly(spark, task)
            else:
                res = sync_standard_table(spark, task)
            results.append(res)
        except Exception as ex:
            print(f"❌ Task {target} failed: {str(ex)}")
            import traceback
            traceback.print_exc()
            results.append(f"❌ Failed: {target} ({source}) - Error: {str(ex)}")
            has_failed = True
            
    print("\n" + "=" * 70)
    print("📊 UNIFIED SPARK JOB SYNC SUMMARY")
    print("=" * 70)
    for r in results:
        print(r)
    print("=" * 70)
    
    if has_failed:
        print("❌ One or more sync tasks failed.")
        sys.exit(1)
    else:
        print("✅ All sync tasks completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
