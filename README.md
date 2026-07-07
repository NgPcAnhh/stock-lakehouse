# 📈 Hệ Thống Phân Tích Dữ Liệu Chứng Khoán: Modern Data Lakehouse & BI Platform (AI Vibecoding Agent)

Dự án này là sự kết hợp giữa **Kiến trúc Modern Data Lakehouse** (ở tầng dữ liệu) và **Nền Tảng Phân Tích Doanh Nghiệp Trực Quan (BI Platform)** tích hợp **AI Agent chuyên biệt (Vibecoding BI Agent)** ở tầng ứng dụng. Hệ thống hỗ trợ người dùng kết nối các nguồn dữ liệu lớn (ClickHouse, PostgreSQL), xây dựng các tập dữ liệu (Datasets), và sử dụng trí tuệ nhân tạo để sinh hoặc điều chỉnh mã giao diện biểu đồ (**vibecoding**) trực tiếp bằng ngôn ngữ tự nhiên.

Dữ liệu chứng khoán và tài chính doanh nghiệp Việt Nam đóng vai trò là nguồn dữ liệu mẫu cốt lõi của Lakehouse phục vụ cho các báo cáo phân tích trên BI Platform.

---

## 🗺️ Sơ đồ Kiến trúc & Luồng Dữ liệu (Data Flow Architecture)

Kiến trúc hệ thống bao gồm hai thành phần chính: **Data Lakehouse Pipeline** (xử lý & lưu trữ dữ liệu thô thành dữ liệu phân tích chuẩn hóa) và **BI Platform Server** (phục vụ nghiệp vụ BI & tích hợp AI Agent sinh code biểu đồ):

```text
========================================================================================================
                              SƠ ĐỒ HẠ TẦNG BIG DATA LAKEHOUSE & BI PLATFORM
========================================================================================================

    [ LUỒNG BATCH: BCTC, Giá EOD, Vĩ mô ]                   [ LUỒNG REAL-TIME: Bảng giá/Khớp lệnh ]
    +-----------------------------------+                   +-------------------------------------+
    |   APIs / VNStock / Web Scraping   |                   |        KAFKA TOPIC (Stream)         |
    +-----------------------------------+                   +-------------------------------------+
                      | (Lập lịch crawl)                                       |
                      v                                                        | (Consume 24/7)
         +-------------------------+                                           v
         |    APACHE AIRFLOW       |                                  +------------------+
         |      SCHEDULER          |                                  |   CLICKHOUSE     |
         +-------------------------+                                  |   KAFKA ENGINE   |
                      | (Ghi file thô)                                +------------------+
                      v                                                        |
         +-------------------------+                                           | (Ghi vào MergeTree)
         |    MINIO LANDING ZONE   |                                           v
         |  (Bucket raw CSV/Parquet) |                                +------------------+
         +-------------------------+                                  |    CLICKHOUSE    |
                      |                                               |  realtime_quotes |
                      | (Đọc & làm sạch)                              +------------------+
                      v                                                        ^
         +-------------------------+                                           |
         |      APACHE SPARK       |                                           | (Query trực tiếp)
         |   (Spark Cmd Server)    |                                           |
         +-------------------------+                                           |
                      | (Ghi Iceberg Table)                                    |
                      v                                                        |
         +-------------------------+                                           |
         |    MINIO WAREHOUSE      | <-----------------------------------------+
         |  (Iceberg stock_db)     | (Tích hợp thông qua clickhouse-iceberg-engine)
         +-------------------------+
                      |
                      v (Phục vụ truy vấn dữ liệu & metadata)
         +-------------------------------------------------------------------------------------+
         |                                  FASTAPI BACKEND API                                |
         |                      ( SQLAlchemy AsyncSession / ClickHouse Driver )                |
         +-------------------------------------------------------------------------------------+
              ^                                  ^                                  ^
              | (REST API / JSON)                | (Lấy Dataset schema & rows)      | (Tương tác UI)
              v                                  v                                  v
         +----------+                     +--------------+                     +---------------+
         | NEXT.JS  |                     |   AI CHART   |                     |  POSTGRESQL   |
         | FRONTEND | <================== |  VIBECODING  | <================== |   (BI HUB &   |
         |  (UI)    |   (JS ECharts code) |  (OpenAI BE) |   (Prompts của User)|  SYSTEM DB)   |
         +----------+                     +--------------+                     +---------------+
========================================================================================================
```

---

## 1. Tổng Quan Kiến Trúc & Công Nghệ

Hệ thống được tổ chức thành các phân vùng công nghệ rõ ràng nhằm đảm bảo hiệu năng xử lý dữ liệu lớn (Big Data) và trải nghiệm người dùng tương tác động mượt mà:

### 1.1. Công nghệ Tầng Dữ liệu (Big Data Lakehouse)
*   **Apache Airflow:** Điều phối (Orchestration) toàn bộ luồng công việc. Airflow lập lịch và giám sát các DAG tải dữ liệu hàng ngày, sau đó gọi REST API tới Spark để kích hoạt các job xử lý tương ứng.
*   **Apache Spark:** Công cụ tính toán phân tán (Processing Engine) xử lý batch và stream. Đọc dữ liệu từ Landing Zone của MinIO, làm sạch, ép kiểu, xử lý trùng lặp và ghi xuống kho lưu trữ dưới định dạng Iceberg.
*   **Apache Iceberg:** Định dạng bảng mở (Open Table Format) mang đến khả năng giao dịch ACID transactions, Time Travel, Schema Evolution trực tiếp trên Object Storage mà không cần cơ sở dữ liệu truyền thống.
*   **MinIO:** Hệ thống Object Storage tương thích chuẩn AWS S3, chia làm 2 bucket: `thongtin-congty-va-bctc` (Landing Zone - dữ liệu thô) và `stock-datalake` (Warehouse Zone - bảng Iceberg đã chuẩn hóa).
*   **Apache Kafka:** Broker luồng dữ liệu bảng điện/giao dịch khớp lệnh trực tuyến thời gian thực.
*   **ClickHouse (OLAP serving):** Đóng vai trò là Serving Engine cho các phân tích dữ liệu lớn. Tích hợp trực tiếp với Datalake thông qua **Iceberg Table Engine** để truy vấn trực tiếp file Parquet trên MinIO mà không cần nạp lại dữ liệu, đồng thời tích hợp **Kafka Engine** để tiêu thụ luồng real-time.

### 1.2. Công nghệ Tầng Ứng dụng & BI Server
*   **FastAPI (Python):** REST API bất đồng bộ (async), độ trễ thấp, kết nối song song ClickHouse và PostgreSQL.
*   **PostgreSQL (OLTP):** Lưu trữ thông tin phân quyền nghiệp vụ BI, cấu hình dashboard/chart và hành vi người dùng.
*   **Redis:** Caching truy vấn dataset và làm message broker cho luồng Websocket cập nhật bảng điện.
*   **OpenAI API (GPT-4o/gpt-5.4-mini):** Trí tuệ nhân tạo làm nhiệm vụ hiểu ý định người dùng, đọc schema dữ liệu và sinh mã cấu hình biểu đồ Apache ECharts dạng JavaScript (**Vibecoding Agent**).
*   **Next.js (React, TypeScript & TailwindCSS):** Giao diện Web App hiển thị bảng biểu đồ phân tích, trang quản trị BI và trình biên tập code trực quan Monaco Editor.
*   **Apache ECharts:** Thư viện vẽ biểu đồ JavaScript hiệu năng cao, được điều khiển động bởi mã code do AI sinh ra.

---

## 2. Các Use-Case Nghiệp Vụ BI & AI Agent

Hệ thống được phát triển chuyên biệt cho nghiệp vụ Business Intelligence với các use-case cốt lõi sau:

### 2.1. Quản lý Kết nối Dữ liệu (Data Sources)
*   Cho phép người dùng kết nối tới các cơ sở dữ liệu khác nhau (ClickHouse, PostgreSQL, MySQL...) bằng cách cấu hình host, port, database name và thông tin đăng nhập.
*   Thông tin kết nối được mã hóa và lưu trữ an toàn trong schema `bi_hub.data_sources`.

### 2.2. Xây dựng Tập dữ liệu (Datasets & Queries)
*   Người dùng có thể viết các câu lệnh SQL để truy vấn và chọn lọc dữ liệu từ các Data Source đã kết nối.
*   Hệ thống tự động phân tích và trích xuất cấu trúc cột (Schema: Tên cột, kiểu dữ liệu) của dataset và lưu trữ cache dữ liệu mẫu phục vụ cho việc thiết kế biểu đồ.

### 2.3. Trợ lý AI Sinh & Tuỳ biến Biểu đồ (AI Vibecoding Agent)
*   **Sinh code biểu đồ lần đầu (First-time generation):** Người dùng nhập yêu cầu bằng ngôn ngữ tự nhiên (Ví dụ: *"Vẽ biểu đồ hình cột so sánh doanh thu và giá vốn của VIC qua các quý"*). AI Agent đọc mô tả yêu cầu kết hợp với **Dataset Schema** (cột, kiểu dữ liệu) và **dữ liệu mẫu (sample rows)** để tự động viết mã JavaScript Apache ECharts hoàn chỉnh (có cấu trúc `return option;` hoặc `return { ... };`).
*   **Cập nhật biểu đồ động (Incremental update):** Người dùng tiếp tục ra lệnh bằng ngôn ngữ tự nhiên (Ví dụ: *"Chuyển trục tung sang bên phải và đổi màu các cột thành màu xanh lá cây"*). AI Agent sẽ đọc đoạn mã JavaScript ECharts hiện tại cùng với yêu cầu mới để viết lại mã nguồn mới một cách chính xác.
*   **Monaco Editor Integration:** Đoạn code do AI sinh ra được hiển thị trực tiếp trên trình biên tập Monaco Editor của Frontend Next.js, cho phép người dùng xem trực quan kết quả render biểu đồ bên cạnh và tự do chỉnh sửa thủ công nếu muốn.

### 2.4. Thiết kế Dashboard kéo thả
*   Người dùng tạo các Dashboard và kéo thả, sắp xếp các biểu đồ (Charts) đã tạo vào giao diện lưới (Layout Grid) theo ý muốn.
*   Hỗ trợ chế độ lọc đồng bộ (Global Filters) theo Mã cổ phiếu, Kỳ báo cáo trên toàn bộ Dashboard.

### 2.5. Phân quyền & Quản lý Không gian làm việc (Workspaces & Permissions)
*   Tổ chức tài nguyên báo cáo theo các Không gian làm việc (Workspaces).
*   Phân quyền chia sẻ báo cáo, biểu đồ cho từng người dùng hoặc nhóm người dùng trong tổ chức.

### 2.6. Giám sát & Đo lường Hành vi (Telemetry & Audit Logs)
*   Theo dõi và lưu vết chi tiết hành động người dùng: lượt xem page, click menu sidebar, lượt tìm kiếm thông tin cổ phiếu, và thời gian duy trì phiên làm việc thực tế (API Heartbeat 30 giây).
*   Giám sát chất lượng đồng bộ của Data Lakehouse (Data Health) và thống kê lỗi hệ thống tự động từ Frontend.

---

## 3. Kiến Trúc Data Lakehouse Chi Tiết

Dữ liệu thô sau khi thu thập được đưa vào quy trình xử lý Medallion tự động:

### 3.1. Mô hình 2 Buckets trên Object Storage MinIO
*   **Landing Zone (Raw Layer):** Lưu trữ trong bucket `thongtin-congty-va-bctc`. File thô tải về (CSV/Parquet) được tổ chức theo cấu trúc phân vùng ngày thu thập: `s3a://thongtin-congty-va-bctc/{tên_bảng}/date=YYYY-MM-DD/`.
*   **Warehouse Zone (Silver/Gold Layer):** Lưu trữ trong bucket `stock-datalake`. Chứa các bảng định dạng mở **Apache Iceberg** được quản lý tại namespace `stock_db`. Iceberg giúp giải quyết triệt để bài toán sửa đổi/xóa dữ liệu và truy vấn ngược thời gian (Time Travel).

### 3.2. Quy Chuẩn Đồng Bộ & Thiết kế Dim/Fact
Quy chuẩn thiết kế được quy định thống nhất trong [datalake_standard.md](file:///d:/project/stock-warehouse/utilities/datalake_standard.md):

| Tên bảng Iceberg đích | Thư mục lưu trữ đích trên MinIO | Phân vùng (Partitioning) | Quy tắc làm sạch & Trùng lặp (Idempotency) |
| :--- | :--- | :--- | :--- |
| `dim_company` | `stock_db.db/dim_company` | Không phân vùng | Ghi đè toàn bộ (`overwrite`) từ phân vùng thô mới nhất |
| `dim_people` | `stock_db.db/dim_people` | Không phân vùng | Ghi đè toàn bộ (`overwrite`) từ phân vùng thô mới nhất |
| `fact_history_price` | `stock_db.db/fact_history_price` | `years(prd_id)` | Upsert theo ngày: Xóa dữ liệu cũ của ngày đó trước khi ghi |
| `fact_market_index` | `stock_db.db/fact_market_index` | Không phân vùng | Upsert theo ngày: Xóa dữ liệu cũ của ngày đó trước khi ghi |
| `fact_financial_reports`| `stock_db.db/fact_financial_reports`| Không phân vùng | Upsert theo khóa: `ticker, year, quarter` |
| `fact_financial_ratios` | `stock_db.db/fact_financial_ratios` | Không phân vùng | Upsert theo khóa: `ticker, year, quarter` |
| `fact_electric_board` | `stock_db.db/fact_electric_board` | `days(prd_id)` | Upsert theo ngày: Xóa dữ liệu cũ của ngày đó trước khi ghi |
| `fact_macro_economy` | `stock_db.db/fact_macro_economy` | Không phân vùng | Upsert theo ngày: Xóa dữ liệu cũ của ngày đó trước khi ghi |
| `fact_vn_macro_yearly` | `stock_db.db/fact_vn_macro_yearly` | Không phân vùng | Ghi đè hoặc Upsert theo năm |
| `fact_global_index` | `stock_db.db/fact_global_index` | Không phân vùng | Upsert theo ngày |
| `fact_news` | `stock_db.db/fact_news` | `months(prd_id)` | Upsert theo ngày |

### 3.3. Quy chuẩn Đặt tên và Định dạng Thời gian
*   **Đổi tên cột thời gian sang `prd_id`:** Tất cả các cột thời gian chính trong bảng timeseries/EOD phải đổi tên thành `prd_id` (Ví dụ: `trading_date` $\rightarrow$ `prd_id`, `published` $\rightarrow$ `prd_id`).
*   **Chuẩn hóa kiểu dữ liệu thời gian:**
    *   Cột chỉ có Ngày (Date-only): Định dạng bắt buộc `yyyy-mm-dd` (Ví dụ: `2026-07-07`).
    *   Cột có độ chính xác Giây (Timestamp): Định dạng bắt buộc `yyyy-mm-dd hh:mm:ss` (Ví dụ: `2026-07-07 09:00:00`).

---

## 4. Lược Đồ Cơ Sở Dữ Liệu Chi Tiết (Database Schemas)

Hệ thống quản lý dữ liệu chặt chẽ qua 3 phân vùng Schema trên PostgreSQL và ClickHouse:

### 4.1. Lược đồ clickhouse OLAP phục vụ phân tích (`stock_db`)
Xem chi tiết các câu lệnh SQL cài đặt trong thư mục [clickhouse/init](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/):
1.  **Các bảng phân tích Iceberg:** Ánh xạ trực tiếp tới thư mục lưu trữ của Iceberg trên MinIO thông qua [01_clickhouse_iceberg_setup.sql](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/01_clickhouse_iceberg_setup.sql) sử dụng `ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/tableName', 'minioadmin', 'minioadmin')`.
2.  **Luồng realtime giao dịch:** Cấu hình trong [02_clickhouse_kafka_setup.sql](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/02_clickhouse_kafka_setup.sql):
    *   `realtime_quotes_ch`: Bảng lưu trữ đích (MergeTree Engine, sắp xếp theo `ticker, prd_id`).
    *   `kafka_realtime_quotes_ch`: Bảng đệm (Kafka Engine tiêu thụ dữ liệu JSON từ topic `market.quotes.raw`).
    *   `mv_kafka_to_realtime_quotes_ch`: Materialized View tự động biến đổi và đẩy dữ liệu từ bảng đệm sang bảng đích.

### 4.2. Lược đồ PostgreSQL nghiệp vụ BI (`bi_hub`)
Tất cả bảng thuộc schema `bi_hub` đều sử dụng khóa chính dạng UUID và được khởi tạo tự động trong [database.py](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/database.py):
*   `bi_hub.workspaces`: Quản lý không gian làm việc (id, name, slug).
*   `bi_hub.data_sources`: Kết nối DB nguồn (id, workspace_id, name, type, host, port, database_name, username, encrypted_password, ssl_config, extra_config).
*   `bi_hub.dataset_folders`: Thư mục quản lý dataset (id, workspace_id, name, parent_id).
*   `bi_hub.datasets`: Tập dữ liệu nghiệp vụ (id, workspace_id, data_source_id, query_id, folder_id, name, query_sql, columns_metadata, sample_data_cache).
*   `bi_hub.queries`: Nhật ký câu lệnh SQL lưu trữ hoặc thực thi (id, workspace_id, data_source_id, sql_text, executed_by).
*   `bi_hub.charts`: Cấu hình biểu đồ (id, workspace_id, dataset_id, name, description, chart_type, js_code, config_json, created_by). *Trường `js_code` lưu trữ đoạn code JavaScript ECharts do AI Vibecoding Agent sinh ra.*
*   `bi_hub.dashboards`: Dashboard tổng hợp (id, workspace_id, name, description, is_public, created_by).
*   `bi_hub.dashboard_charts`: Bảng trung gian liên kết Dashboard với Chart kèm vị trí và kích thước trên lưới layout (dashboard_id, chart_id, layout_x, layout_y, layout_w, layout_h).
*   `bi_hub.chart_permissions`: Phân quyền người dùng trên biểu đồ.

### 4.3. Lược đồ PostgreSQL quản trị & giám sát hành vi (`system`)
Định nghĩa trong [auth_tables.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/auth_tables.sql) và [schema_system.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/schema_system.sql):
*   `system.roles`: Danh sách vai trò (`user`, `admin`, `moderator`).
*   `system.users`: Tài khoản người dùng, băm mật khẩu, thông tin kích hoạt 2FA.
*   `system.refresh_tokens` & `system.password_reset_tokens`: Quản lý phiên đăng nhập và mã reset mật khẩu.
*   `system.session_logs`: Nhật ký thời lượng phiên sử dụng (heartbeat 30 giây gửi từ FE).
*   `system.login_logs`: Nhật ký đăng nhập (phương thức local/google, thành công/thất bại, IP, thiết bị).
*   `system.page_views`, `system.sidebar_clicks`, `system.error_logs`: Telemetry theo dõi hoạt động người dùng và lỗi trên giao diện.

### 4.4. Lược đồ dữ liệu mẫu chứng khoán (`hethong_phantich_chungkhoan`)
*   Bản sao lưu dữ liệu tài chính phục vụ cho các truy vấn SQL tạo dataset mẫu. Lược đồ cấu trúc chi tiết được định nghĩa trong [schema_v1.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/schema_v1.sql) (gồm các bảng `history_price`, `bctc`, `financial_ratio`, `company_overview`...).

---

## 5. Cấu Trúc Thư Mục Dự Án (Project Structure)

Dự án được tổ chức cấu trúc phân tách rõ ràng giữa phân hệ dữ liệu lớn và phân hệ ứng dụng web:

```text
stock-warehouse/
├── bi-platform/                        # NỀN TẢNG BI & AI VIBECODING AGENT
│   ├── BE/                             # Backend API (FastAPI)
│   │   ├── app/
│   │   │   ├── api/                    # Định nghĩa các router REST API chung
│   │   │   ├── core/                   # Cấu hình hệ thống, JWT, cấu hình LLM/OpenAI
│   │   │   │   └── llm.py              # Tiện ích gọi OpenAI API sinh text/structured output/embedding
│   │   │   ├── database/               # Kết nối PostgreSQL, ClickHouse và các file SQL khởi tạo
│   │   │   │   ├── database.py         # Khởi tạo DB, tạo schema bi_hub & system, seed dữ liệu mẫu
│   │   │   │   ├── auth_tables.sql     # SQL Schema bảng phân quyền, đăng nhập
│   │   │   │   ├── schema_system.sql   # SQL Schema bảng tracking hành vi, sessions, alerts
│   │   │   │   └── schema_v1.sql       # SQL Schema bảng dữ liệu mẫu chứng khoán
│   │   │   ├── modules/                # Thư mục chứa các module nghiệp vụ chính
│   │   │   │   ├── auth/               # Module đăng nhập, đăng ký, 2FA
│   │   │   │   ├── bi/                 # MODULE BI CORE & AI CHART AGENT
│   │   │   │   │   ├── charts/         # Xử lý sinh code biểu đồ và lưu trữ Chart
│   │   │   │   │   │   └── ai_service.py # AI Chart Agent - sinh/chỉnh sửa JavaScript ECharts qua OpenAI
│   │   │   │   │   ├── dashboards/     # Xử lý Dashboard, layout grid kéo thả
│   │   │   │   │   ├── datasets/       # Xử lý Dataset, cache dữ liệu mẫu
│   │   │   │   │   ├── data_sources/   # Kết nối các Data Source (Postgres, Clickhouse...)
│   │   │   │   │   ├── queries/        # Xử lý thực thi SQL thô
│   │   │   │   │   └── models/         # Các SQLAlchemy ORM models (Workspace, DataSource, Chart...)
│   │   │   │   └── admin/              # Module quản trị, thống kê KPI hệ thống
│   │   │   └── main.py                 # File khởi chạy chính của API Backend
│   │   ├── requirements.txt            # Thư viện Python Backend
│   │   └── .env                        # File biến môi trường của Backend API
│   ├── FE/                             # Frontend UI (Next.js App Router)
│   │   ├── app/                        # Giao diện màn hình chính
│   │   │   ├── (auth)/                 # Giao diện Đăng nhập / Đăng ký / 2FA
│   │   │   ├── market/                 # Giao diện phân tích thị trường & bảng giá
│   │   │   ├── stock/                  # Giao diện phân tích doanh nghiệp chi tiết
│   │   │   ├── portfolio/              # Giao diện danh mục đầu tư
│   │   │   └── settings/               # Giao diện cài đặt kết nối dữ liệu
│   │   ├── components/                 # Các UI Components dùng chung và các chart wrapper
│   │   ├── package.json                # Quản lý thư viện Next.js
│   │   └── .env.local                  # File cấu hình biến môi trường Frontend
│   ├── md/                             # Tài liệu BA mẫu hướng dẫn Vibecoding cho từng loại ngành
│   │   ├── tài liệu dashboard bctc phi tài chính.md
│   │   ├── tài liệu dashboard bctc tài chính.md
│   │   ├── tài liệu dashboard bctc ngân hàng.md
│   │   └── tài liệu dashboard bctc bảo hiểm.md
│   ├── docker-compose.yml              # Docker compose cho riêng cụm BI Platform
│   └── README.md                       # Tài liệu đặc tả kỹ thuật nội bộ của BI Platform
│
├── data-pipeline/                      # HẠ TẦNG DATA LAKEHOUSE (ETL)
│   ├── etl/                            # Thư mục chính điều phối cụm ETL Datalake
│   │   ├── airflow/                    # Apache Airflow (Dags, plugins, Dockerfile)
│   │   │   └── dags/                   # 22 DAGs Airflow crawl, đồng bộ dữ liệu mẫu
│   │   ├── clickhouse/                 # Clickhouse SQL init scripts
│   │   │   └── init/                   # Script ánh xạ Iceberg & Kafka
│   │   ├── spark/                      # Apache Spark (Config, spark_cmd_server, Spark jobs)
│   │   │   ├── apps/
│   │   │   │   ├── minio_to_iceberg/   # Spark Job batch ghi dữ liệu thô sang Iceberg
│   │   │   │   ├── kafka_to_iceberg/   # Spark Streaming ghi luồng Kafka sang Iceberg
│   │   │   │   └── server/             # Spark Command Server API lắng nghe Airflow
│   │   ├── kafka/                      # Cấu hình cụm Kafka
│   │   ├── minio/                      # Lưu trữ MinIO data
│   │   ├── docker-compose.yaml         # Docker compose khởi chạy cụm Datalakehouse
│   │   └── .env                        # Biến môi trường Datalakehouse
│
├── utilities/                          # TÀI LIỆU QUY CHUẨN DỮ LIỆU
│   ├── architecture.txt                # Sơ đồ hạ tầng dạng văn bản thô
│   ├── datalake_standard.md            # Quy chuẩn đặt tên prd_id và định dạng thời gian
│   └── postgres_schema.csv             # Chi tiết các trường cột của Database PostgreSQL
│
├── docs/                               # TÀI LIỆU HÌNH ẢNH
│   └── images/                         # Sơ đồ kiến trúc tổng thể
│
├── docker-compose.yml                  # FILE COMPOSE ROOT (Gộp cả cụm ETL và BI bằng include)
└── .gitignore                          # Cấu hình bỏ qua tệp tin Git
```

---

## 6. Hướng Dẫn Cài Đặt & Khởi Chạy

### 6.1. Khởi chạy toàn bộ hệ thống bằng Docker Compose (Khuyên dùng)
Hệ thống sử dụng tính năng `include` để gộp toàn bộ dịch vụ của cả 2 cụm (Data Pipeline và BI Platform) chạy chung dưới một network.

#### Yêu cầu hệ thống:
*   Docker & Docker Compose đã được cài đặt và đang chạy.
*   Cấu hình RAM trống tối thiểu: **8GB**.

#### Các bước khởi chạy:
1.  **Cấu hình môi trường:**
    *   Tạo file `.env` từ file `.env.example` trong [data-pipeline/etl](file:///d:/project/stock-warehouse/data-pipeline/etl/):
        ```bash
        cd data-pipeline/etl
        cp .env.example .env
        ```
    *   Tạo file `.env` từ file `.env.example` trong [bi-platform/BE](file:///d:/project/stock-warehouse/bi-platform/BE/):
        ```bash
        cd ../../bi-platform/BE
        cp .env.example .env
        ```
    *   Tạo file `.env.local` từ file `.env.example` trong [bi-platform/FE](file:///d:/project/stock-warehouse/bi-platform/FE/):
        ```bash
        cd ../FE
        cp .env.example .env.local
        ```
        *Quan trọng: Điền khóa `OPENAI_API_KEY` vào file `.env` của Backend để sử dụng tính năng AI Vibecoding Agent sinh biểu đồ.*

2.  **Khởi chạy Docker từ thư mục gốc:**
    Quay trở lại thư mục gốc của dự án và chạy lệnh:
    ```bash
    cd ../..
    docker compose up -d
    ```

3.  **Kiểm tra trạng thái truy cập các cổng dịch vụ:**
    *   **BI Platform Frontend UI:** `http://localhost:3000`
    *   **BI Platform Backend API Docs:** `http://localhost:8000/docs`
    *   **Apache Airflow Webserver (ETL Orchestrator):** `http://localhost:8080` (Tài khoản: `airflow` / `airflow`)
    *   **MinIO Console (S3 Object Storage):** `http://localhost:9001` (Tài khoản: `minioadmin` / `minioadmin`)
    *   **ClickHouse Server:** Cổng HTTP `8123`, Cổng Native `9000`

---

### 6.2. Cài đặt thủ công cho Phát triển (Development Mode)
Trong trường hợp bạn muốn chạy Backend và Frontend của BI Platform trực tiếp ở môi trường cục bộ để phát triển code nhanh:

#### Bước 1: Khởi chạy các cơ sở dữ liệu và cụm hạ tầng
Chạy các Container của MinIO, Spark, ClickHouse, Postgres, Kafka và Airflow:
```bash
cd data-pipeline/etl
docker compose up -d
```

#### Bước 2: Chạy Backend FastAPI (Python)
1.  Di chuyển vào thư mục Backend:
    ```bash
    cd ../../bi-platform/BE
    ```
2.  Tạo và kích hoạt môi trường ảo Python:
    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # macOS/Linux:
    source .venv/bin/activate
    ```
3.  Cài đặt các thư viện:
    ```bash
    pip install -r requirements.txt
    ```
4.  Cấu hình file `.env` trỏ tới cổng tương ứng (ví dụ `localhost:5432` cho PostgreSQL, `localhost:8123` cho ClickHouse).
5.  Khởi chạy server uvicorn:
    ```bash
    uvicorn app.main:app --reload
    ```
    *Backend sẽ chạy tại: `http://localhost:8000`*

#### Bước 3: Chạy Frontend Next.js
1.  Mở một cửa sổ terminal mới và di chuyển vào thư mục Frontend:
    ```bash
    cd bi-platform/FE
    ```
2.  Cài đặt các thư viện Node.js:
    ```bash
    npm install
    ```
3.  Kiểm tra cấu hình biến môi trường trong `.env.local` trỏ về Backend API:
    ```text
    NEXT_PUBLIC_API_URL=http://localhost:8000
    ```
4.  Khởi chạy dev server:
    ```bash
    npm run dev
    ```
    *Frontend sẽ chạy tại: `http://localhost:3000`*
