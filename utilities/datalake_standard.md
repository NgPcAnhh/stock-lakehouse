# Tiêu chuẩn Thiết kế và Đồng bộ Data Lakehouse
Tài liệu này đóng vai trò là **Skill (Quy chuẩn hệ thống)** hướng dẫn cách phân chia Dim/Fact, quy tắc đặt tên cột và định dạng thời gian khi code các luồng Airflow ETL, Spark và ClickHouse.

---

## 1. Kiến trúc lưu trữ 2 Buckets
Hệ thống tuân thủ mô hình **Medallion Architecture** sử dụng 2 bucket độc lập trên MinIO:
* **Raw/Landing Zone (Dữ liệu thô)**: Lưu trữ tại bucket `thongtin-congty-va-bctc`. Chứa file CSV/Parquet thô cào về trực tiếp từ nguồn API.
* **Warehouse Zone (Dữ liệu chuẩn hóa)**: Lưu trữ tại bucket `stock-datalake`. Chứa các bảng định dạng **Apache Iceberg** được quản lý tại namespace `stock_db`.

---

## 2. Mô hình Dim/Fact & Cấu trúc ánh xạ bảng Iceberg
Dưới đây là thiết kế chi tiết ánh xạ các bảng từ Postgres/Landing thô sang bảng Iceberg mới:

| STT | Bảng thô (Postgres/Landing) | Tên bảng Iceberg mới | Thư mục lưu trữ đích (`stock-datalake`) | Phân vùng (Partitioning) |
| :--- | :--- | :--- | :--- | :--- |
| **1** | `company_overview` | `dim_company` | `stock_db.db/dim_company` | Không phân vùng |
| **2** | `people` | `dim_people` | `stock_db.db/dim_people` | Không phân vùng |
| **3** | `history_price` | `fact_history_price` | `stock_db.db/fact_history_price` | `years(prd_id)` (Phân vùng Năm) |
| **4** | `market_index` | `fact_market_index` | `stock_db.db/fact_market_index` | Không phân vùng |
| **5** | `bctc` | `fact_financial_reports` | `stock_db.db/fact_financial_reports` | Không phân vùng |
| **6** | `financial_ratio` | `fact_financial_ratios` | `stock_db.db/fact_financial_ratios` | Không phân vùng |
| **7** | `electric_board` | `fact_electric_board` | `stock_db.db/fact_electric_board` | `days(prd_id)` (Phân vùng Ngày) |
| **8** | `macro_economy` | `fact_macro_economy` | `stock_db.db/fact_macro_economy` | Không phân vùng |
| **9** | `vn_macro_yearly` | `fact_vn_macro_yearly` | `stock_db.db/fact_vn_macro_yearly` | Không phân vùng |
| **10**| `global_index` | `fact_global_index` | `stock_db.db/fact_global_index` | Không phân vùng |
| **11**| `news` | `fact_news` | `stock_db.db/fact_news` | `months(prd_id)` (Phân vùng Tháng) |

---

## 3. Quy chuẩn Đặt tên cột thời gian (`prd_id`)
Để đồng bộ hóa thiết kế mô hình dữ liệu chuỗi thời gian (time-series), tất cả các cột mốc thời gian chính của bản ghi đại diện cho phiên giao dịch/chu kỳ báo cáo **bắt buộc phải đổi tên thành `prd_id`** (ngoại trừ các cột kiểm toán hệ thống như `import_time`, `created_at`, `inserted_at`).

Bảng ánh xạ đổi tên cột chi tiết:
* `history_price.trading_date` $\rightarrow$ **`prd_id`**
* `macro_economy.date` $\rightarrow$ **`prd_id`**
* `market_index.trading_date` $\rightarrow$ **`prd_id`**
* `news.published` $\rightarrow$ **`prd_id`**
* `realtime_quotes.ts` $\rightarrow$ **`prd_id`**
* `electric_board.trading_date` $\rightarrow$ **`prd_id`**

---

## 4. Quy chuẩn Định dạng dữ liệu thời gian (Datetime Formats)
Khi xử lý dữ liệu trong Airflow DAG hoặc Spark, giá trị của cột `prd_id` phải được chuẩn hóa đúng định dạng sau trước khi ghi xuống Iceberg và đồng bộ vào ClickHouse:

1. **Đối với các cột chỉ có Ngày (Date-only)**:
   * **Định dạng bắt buộc**: `yyyy-mm-dd` (Ví dụ: `2026-06-10`).
   * Áp dụng cho: `fact_history_price`, `fact_market_index`, `fact_electric_board`, `fact_macro_economy`.
2. **Đối với các cột có độ chính xác theo Giây (Second-precision / Timestamp)**:
   * **Định dạng bắt buộc**: `yyyy-mm-dd hh:mm:ss` (Ví dụ: `2026-06-10 14:54:00`).
   * Áp dụng cho: `realtime_quotes`, `fact_news`.
