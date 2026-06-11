'''
=====sync_vn_macro_yearly_to_iceberg.py =======
Đối tượng xử lý: Chỉ chạy riêng cho dữ liệu vĩ mô năm của Việt Nam (vn_macro_yearly).
Đặc điểm dữ liệu nguồn: Tệp CSV thô từ MinIO của bảng này cực kỳ phức tạp và không theo chuẩn database thông thường:
Tên chỉ số là tiếng Việt có dấu và chứa ký tự đặc biệt: Ví dụ: “Tổng sản phẩm trong nước (GDP)”, “Tốc độ tăng trưởng GDP (%)”. Chúng ta không thể dùng các chuỗi này làm tên cột trong bảng Iceberg. Do đó script cần hàm slugify() riêng để chuyển chúng thành tên cột SQL hợp lệ (e.g. tong_san_pham_trong_nuoc_gdp).
Dữ liệu dạng bảng ngang (Wide Table): Các năm được chia theo từng cột (YR2014, YR2015, YR2016,...). Để lưu vào database phân tích, chúng ta cần xoay trục (Transpose/Pivot) đưa các cột năm thành dòng giá trị (cột year) và chuyển các chỉ số vĩ mô thành cột dọc. Script bắt buộc phải dùng Pandas (pdf.T) để thực hiện bước xử lý phức tạp này.
Chế độ ghi đè (overwrite): Dữ liệu vĩ mô cập nhật theo năm và có cấu trúc thay đổi động khi xoay trục nên script chọn ghi đè toàn bộ bảng thay vì chèn nối tiếp (append).
'''

import argparse
import sys
import unicodedata
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def slugify(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    ascii_text = re.sub(r"\(.*?\)", "", ascii_text)
    ascii_text = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_text)
    parts = ascii_text.strip().lower().split()
    slug = "_".join(parts)
    return slug.strip("_") or "unknown"

def main():
    parser = argparse.ArgumentParser(description="Sync Vietnam Macro Yearly from MinIO Landing to Iceberg")
    parser.add_argument("--source", required=True, help="S3 Source directory")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("Sync-vn_macro_yearly") \
        .getOrCreate()
        
    try:
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(args.source)
            
        if df.count() == 0:
            print("⚠️ Source is empty.")
            sys.exit(0)
            
        # Standardize columns
        df = df.select([col(c).alias(c.strip()) for c in df.columns])
        
        # 1. Clean 'chi_so' names and slugify them
        data = df.collect()
        slug_map = {}
        for row in data:
            indicator = row['chi_so']
            if indicator:
                slug_map[indicator] = slugify(indicator)
                
        # 2. Reshape wide (YR2014, etc.) to long format
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
        
        # Convert all to float except year
        for c in pdf_t.columns:
            if c != "year":
                pdf_t[c] = pd.to_numeric(pdf_t[c], errors="coerce")
                
        # Convert back to Spark DataFrame
        spark_df = spark.createDataFrame(pdf_t)
        
        # Align with target schema
        target_table_path = "stock_catalog.stock_db.fact_vn_macro_yearly"
        target_df = spark.read.table(target_table_path)
        
        # Add missing columns
        for field in target_df.schema:
            if field.name not in spark_df.columns:
                spark_df = spark_df.withColumn(field.name, lit(None).cast(field.dataType))
                
        # Reorder columns
        spark_df = spark_df.select(*target_df.columns)
        
        # Write (Overwrite)
        print(f"Writing pivoted data to {target_table_path}...")
        spark_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .save(target_table_path)
            
        print("✅ Vietnam macro yearly sync completed successfully!")
    except Exception as e:
        print(f"❌ Error during sync: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
