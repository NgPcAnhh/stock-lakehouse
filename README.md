# 📈 Hệ Thống Phân Tích Chứng Khoán Toàn Diện (Stock Warehouse & BI Platform)

Hệ thống phân tích chứng khoán toàn diện (Stock Warehouse) kết hợp giữa kiến trúc **Modern Data Lakehouse** hiện đại (ở tầng dữ liệu) và **Nền Tảng Phân Tích Doanh Nghiệp Trực Quan (BI Platform)** (ở tầng ứng dụng). Hệ thống cung cấp giải pháp khép kín từ việc thu thập dữ liệu tự động, xử lý dữ liệu lớn phân tán, lưu trữ tối ưu hóa cho phân tích (OLAP), lưu trữ nghiệp vụ (OLTP), cho đến hiển thị biểu đồ phân tích thời gian thực và tích hợp trợ lý ảo thông minh RAG Chatbot.

---

## 🗺️ Sơ đồ Kiến trúc & Luồng Dữ liệu (Data Flow Architecture)

Dưới đây là sơ đồ luồng dữ liệu tổng quan từ nguồn thu thập (Ingestion) qua các tầng lưu trữ đến lớp hiển thị ứng dụng (BI Platform) và Trợ lý AI:

```text
========================================================================================================
                                SƠ ĐỒ TỔNG THỂ HẠ TẦNG BIG DATA & BI PLATFORM
                       (Tích hợp luồng Real-time Kafka và luồng Batch Airflow)
========================================================================================================

    [ LUỒNG REAL-TIME: Dữ liệu giao dịch ]                  [ LUỒNG BATCH: BCTC, Giá Daily, Vĩ mô ]
    +------------------------------------+                  +------------------------------------+
    |         KAFKA TOPIC (Stream)       |                  |      APIs / VNStock / Web Scraping |
    +------------------------------------+                  +------------------------------------+
         |                            |                                       |
         | (Consume 24/7)             | (Consume 24/7)                        | (Crawl & Ingest định kỳ)
         v                            v                                       v
  +--------------+             +--------------+                        +--------------+
  |    PYTHON    |             |  CLICKHOUSE  |                        |   AIRFLOW    | (Chạy các DAG crawl
  |   CONSUMER   |             | KAFKA ENGINE |                        |  SCHEDULER   |  như bctc, price,...)
  | (PyIceberg)  |             | (Tích hợp)   |                        +--------------+
  +--------------+             +--------------+                               |
         |                            |                                       | (Ghi file thô)
         |                            |                                       v
         |                            |                        +------------------------------+
         |                            |                        |      MINIO RAW BUCKET        |
         |                            |                        |  (Lưu trữ file thô CSV/PQ)   |
         |                            |                        +------------------------------+
         |                            |                                       |
         | (Ghi Iceberg Table)        |                                       | (Đọc file & chuyển đổi)
         v                            |                                       v
  +--------------+                    |                        +------------------------------+
  |    MINIO     | <------------------+----------------------- |  SPARK CLUSTER (Submit Jobs) |
  |  ICEBERG-    | (Đồng bộ dữ liệu nóng/lịch sử)              |  - Chuyển CSV -> Parquet     |
  |  WAREHOUSE   |                                             |  - Ghi Iceberg (ACID, Merge) |
  +--------------+                                             +------------------------------+
         ^                                                                    |
         | (Đọc phân tán On-demand)                                           | (Ghi / Đồng bộ)
         |                                                                    v
  +--------------+                                             +------------------------------+
  |    SPARK     |                                             |     CLICKHOUSE OLAP DB       |
  |   CLUSTER    |                                             | - Truy vấn trực tiếp Iceberg |
  |  (User/ML)   |                                             | - Engine: Iceberg / MergeTree|
  +--------------+                                             +------------------------------+
                                                                              |
                                                                              v
                                                                   +----------------------+
                                                                   |     FASTAPI API      | <---> [Redis Cache]
                                                                   +----------------------+
                                                                       |              ^
                                                                       | (REST / WS)  | (Text-to-SQL / Embeddings)
                                                                       v              v
                                                                 +-----------+  +---------------+
                                                                 | NEXT.JS   |  | RAG CHATBOT   |
                                                                 | FRONTEND  |  | (FAISS/GPT-4o)|
                                                                 +-----------+  +---------------+
========================================================================================================
```

Sơ đồ chi tiết hình ảnh luồng dữ liệu hệ thống được đặt tại: [docs/images/data_flow_architecture.png](file:///d:/project/stock-warehouse/docs/images/data_flow_architecture.png)

---

## 1. Tổng Quan Kiến Trúc & Công Nghệ

Hệ thống được thiết kế theo mô hình **Client-Server** kết hợp kiến trúc hướng dịch vụ **Microservices-oriented** cho các pipeline dữ liệu và các tác vụ trí tuệ nhân tạo (AI). 

### 1.1. Công nghệ Tầng Dữ liệu (Big Data & Lakehouse)
*   **Apache Airflow:** Hệ thống điều phối (Orchestration) trung tâm, lập lịch chạy các DAG crawl dữ liệu tài chính định kỳ và gọi API điều khiển Spark.
*   **Apache Spark:** Động cơ tính toán phân tán (Processing Engine), đảm nhận việc chuyển đổi định dạng dữ liệu (CSV sang Parquet), làm sạch dữ liệu, xử lý trùng lặp và đồng bộ vào định dạng bảng mở.
*   **Apache Iceberg:** Định dạng bảng mở (Open Table Format) lưu trữ trực tiếp trên Object Storage, cung cấp các tính năng giao dịch ACID transactions, Time Travel (truy vấn lịch sử bảng), Schema Evolution (tiến hóa lược đồ bảng) và cập nhật dữ liệu tin cậy.
*   **MinIO:** Hệ thống lưu trữ đối tượng (Object Storage) tương thích chuẩn AWS S3, chia làm 2 phân vùng (Raw Landing Zone và Warehouse Silver/Gold Zone).
*   **Apache Kafka:** Hệ thống nhắn tin phân tán (Message Broker) chịu trách nhiệm vận chuyển luồng dữ liệu bảng giá/khớp lệnh khớp thời gian thực với độ trễ cực thấp.
*   **ClickHouse (OLAP Engine):** Cơ sở dữ liệu cột hiệu năng cao sử dụng để lưu trữ tập trung dữ liệu phục vụ các truy vấn phân tích, thống kê lớn với tốc độ mili-giây. ClickHouse tích hợp trực tiếp với Datalake qua **Iceberg Engine** và tích hợp luồng real-time qua **Kafka Engine**.

### 1.2. Công nghệ Tầng Ứng Dụng (BI Platform & API)
*   **Next.js (React & TypeScript):** Framework hiện đại xây dựng giao diện người dùng trực quan, tối ưu SEO, hỗ trợ Server-Side Rendering (SSR).
*   **TailwindCSS & ECharts/Recharts:** Cung cấp giao diện responsive mượt mà và các bộ biểu đồ kỹ thuật phân tích trực quan sinh động.
*   **FastAPI (Python):** Framework REST API bất đồng bộ (async) hiệu năng cao, độ trễ cực thấp, giao tiếp đa kết nối.
*   **PostgreSQL (OLTP Engine & DWH):** Đóng vai trò là Database lưu trữ thông tin nghiệp vụ, dữ liệu tài khoản, phân quyền, cấu hình cá nhân hóa hệ thống, logs tương tác và là Database Context cho Chatbot.
*   **Redis:** Bộ nhớ đệm (Caching) giảm tải truy vấn lặp và là Message Broker hỗ trợ luồng truyền Websocket thời gian thực.
*   **Vector Database (FAISS):** Lưu trữ metadata schema hệ thống và các tài liệu tri thức phục vụ cho kiến trúc RAG Chatbot.
*   **OpenAI API (GPT-4o-mini & Text-Embedding-3-small):** Trí tuệ nhân tạo phục vụ mô hình ngôn ngữ lớn (LLM) để sinh mã SQL truy vấn dữ liệu và lập luận báo cáo tài chính.

---

## 2. Các Use-Case Hệ Thống (Use-Case List)

Hệ thống được phát triển hoàn thiện để phục vụ các nhóm chức năng cốt lõi sau của nhà đầu tư:

### 2.1. Quản trị Người dùng & Bảo mật (Auth & Security)
*   **Xác thực tài khoản:** Đăng ký, đăng nhập cục bộ trả về JWT Token bảo mật, đổi mật khẩu và khôi phục tài khẩu qua mã gửi Email.
*   **Đăng nhập bên thứ ba:** Tích hợp Google OAuth để đăng nhập nhanh chóng.
*   **Xác thực 2 yếu tố (2FA / TOTP):** Tích hợp ứng dụng xác thực như Google Authenticator bằng cách sinh mã QR và TOTP Secret.
*   **Bảng quản trị (Admin Panel):** Quản trị viên theo dõi KPI hệ thống (số lượng người dùng, tần suất đăng nhập, session online) và phân quyền vai trò (`user`, `admin`, `moderator`).

### 2.2. Bảng giá & Diễn biến Thị trường (Real-time Market Board)
*   **Bảng giá trực tuyến:** Kết nối qua WebSockets để truyền tải dữ liệu khớp lệnh, bước giá chờ mua/bán chi tiết thời gian thực với hiệu ứng đổi màu nhấp nháy theo biên độ giá.
*   **Chỉ số thị trường:** Hiển thị điểm số biến động của các chỉ số lớn: VNINDEX, HNXINDEX, UPCOM, VN30.
*   **Dòng tiền thị trường:** Trực quan hóa bản đồ nhiệt (Heatmap) theo phân ngành và mã cổ phiếu (kích thước ô là thanh khoản, màu sắc là % tăng/giảm), phân bổ dòng tiền và các mã đóng góp điểm số lớn nhất vào VNIndex.
*   **Dữ liệu vĩ mô:** Biểu đồ tương quan các tài sản tài chính vĩ mô thế giới (Vàng - XAU, Dầu thô - Brent Oil, Chỉ số Dollar - DXY, US Bond 10Y) và số liệu kinh tế vĩ mô Việt Nam hàng năm.

### 2.3. Tra cứu & Phân tích Cổ phiếu (Stock Analysis)
*   **Hồ sơ doanh nghiệp:** Tra cứu thông tin cơ bản công ty, danh sách ban lãnh đạo, cơ cấu sở hữu cổ đông và lịch sử sự kiện (chi trả cổ tức, phát hành cổ phiếu).
*   **Biểu đồ kỹ thuật:** Tích hợp biểu đồ hình nến lịch sử (OHLCV) với nhiều khung thời gian khác nhau.
*   **Phân tích báo cáo tài chính (BCTC):** Xem bảng cân đối kế toán, kết quả kinh doanh và lưu chuyển tiền tệ. Lược đồ báo cáo tự động điều chỉnh linh hoạt theo mô hình doanh nghiệp: *Phi tài chính, Ngân hàng, Chứng khoán, và Bảo hiểm*.
*   **Chỉ số tài chính:** Phân tích các chỉ số định giá (P/E, P/B, EPS), sinh lời (ROE, ROA, biên lợi nhuận), đòn bẩy tài chính và chu kỳ luân chuyển tiền tệ.
*   **So sánh đối thủ:** So sánh trực diện các chỉ số sức mạnh tài chính của doanh nghiệp với các đối thủ cùng phân ngành ICB.
*   **Ước lượng định giá:** Tích hợp mô hình định giá DCF (Chiết khấu dòng tiền), DDM (Chiết khấu cổ tức), P/E và P/B Band lịch sử. Biểu diễn kết quả dưới dạng biểu đồ *Football Field* trực quan.
*   **Phân tích định lượng (Quant):** Tính toán hệ số Sharpe, Value at Risk (VaR) đo lường rủi ro danh mục, mô phỏng Monte Carlo để dự đoán xác suất đường đi của giá cổ phiếu.
*   **Phân tích kỹ thuật (Technical Gauge):** Client-side engine tự động tính toán các chỉ báo kỹ thuật (SMA, EMA, RSI, MACD, Stochastic, ADX...) và chấm điểm tổng hợp đưa ra khuyến nghị (Mua mạnh, Mua, Trung lập, Bán, Bán mạnh) trên đồng hồ ECharts.

### 2.4. Phân tích Sắc thái Tin tức (News Sentiment Analysis)
*   Tự động crawl tin tức thị trường và doanh nghiệp hàng giờ.
*   Sử dụng mô hình ngôn ngữ **PhoBERT** (`wonrax/phobert-base-vietnamese-sentiment`) tối ưu cho tiếng Việt để đánh giá sắc thái bài báo, quy đổi điểm tin cậy sang thang điểm số thực `[-100.0, 100.0]` (Điểm dương biểu thị tin tích cực, điểm âm biểu thị tin tiêu cực).
*   Sử dụng thuật toán so khớp regex phân loại tin tức vào 19 nhóm ngành cấp 2 theo chuẩn ICB để đưa ra biểu đồ chỉ số tâm lý thị trường tổng hợp và tâm lý từng ngành cụ thể.

### 2.5. Quản lý Danh mục & Cảnh báo (Portfolio & Alerts)
*   **Quản lý danh mục:** Tạo nhiều danh mục đầu tư, thêm lịch sử giao dịch mua/bán để tính toán giá vốn trung bình và tỷ lệ lãi/lỗ (P/L) thời gian thực.
*   **Đo lường rủi ro:** Phân tích hệ số Beta và VaR của toàn bộ danh mục đầu tư.
*   **Cảnh báo giá:** Thiết lập cảnh báo khi giá cổ phiếu chạm, vượt hoặc giảm dưới một ngưỡng giá định sẵn, hệ thống sẽ gửi thông báo đẩy (alerts).

### 2.6. Trợ lý ảo AI Chatbot (StockPilot)
*   Sử dụng mô hình **RAG Multi-Agent** thông minh để trả lời các câu hỏi về tài chính qua ngôn ngữ tự nhiên:
    *   **Search Mode (Fast-path):** Tự động nhận diện ý định hỏi đáp số liệu thô hoặc biểu đồ đơn giản. Hệ thống thực hiện Text-to-SQL kết hợp Context Schema từ Vector DB (FAISS) để sinh mã SQL PostgreSQL, truy vấn nhanh và hiển thị bảng dữ liệu mà không cần thông qua LLM sinh văn bản, giúp tối ưu chi phí và loại bỏ hiện tượng ảo giác (hallucination).
    *   **Analysis Mode (Agentic-path):** Dành cho các câu hỏi phân tích, so sánh. **Analyst Agent** (Bộ điều phối) lập kế hoạch nghiệp vụ, kích hoạt **Data Retriever Agent** để sinh và thực thi đồng thời chuỗi nhiều câu lệnh SQL (quét dữ liệu chuỗi thời gian, cùng kỳ YoY, đối thủ cùng ngành). Dữ liệu JSON trả về được chuyển qua **Insight Agent** lập luận phân tích theo cấu trúc chuẩn (Tóm tắt, Phân tích chi tiết xu hướng/sinh lời/rủi ro, Nhận xét & Lưu ý) và trả về bài viết chuyên nghiệp kèm bảng dữ liệu minh họa.

---

## 3. Kiến Trúc Data Lakehouse Chi Tiết

Hệ thống Data Lakehouse được thiết kế tối ưu tuân thủ mô hình **Medallion Architecture** gồm hai vùng lưu trữ độc lập trên Object Storage MinIO và được điều phối, xử lý tự động:

```text
+-----------------------+      +---------------------------+      +-------------------------------+
|     LANDING ZONE      |      |      PROCESSING ZONE      |      |        WAREHOUSE ZONE         |
|                       |      |                           |      |                               |
|   MinIO Raw Bucket    | ---> |    Apache Spark Jobs      | ---> |     MinIO Warehouse Bucket    |
| (Raw CSV/Parquet/JSON)|      | (Clean, Transform, Match) |      | (Apache Iceberg Format tables)|
+-----------------------+      +---------------------------+      +-------------------------------+
                                             |                                    |
                                             v                                    v
                               +---------------------------+      +-------------------------------+
                               |    Spark Cmd Server       |      |       ClickHouse Engine       |
                               | (Orchestrated by Airflow) |      |   (Direct Iceberg Querying)   |
                               +---------------------------+      +-------------------------------+
```

### 3.1. Phân Phối Tầng Lưu Trữ (2 Buckets trên MinIO)
1.  **Raw/Landing Zone (Dữ liệu thô):** Lưu trữ tại bucket `thongtin-congty-va-bctc`. Chứa các file CSV, Parquet, JSON tải về trực tiếp từ nguồn API của crawler, phân chia thư mục (partitioned) theo ngày/giờ thu thập (ví dụ: `daily_price/date=2026-07-07/data.csv`).
2.  **Warehouse Zone (Dữ liệu chuẩn hóa):** Lưu trữ tại bucket `stock-datalake`. Chứa các bảng định dạng **Apache Iceberg** được quản lý tại namespace `stock_db`. Iceberg lưu trữ metadata chi tiết cho phép truy vấn trực tiếp dạng bảng quan hệ với đầy đủ tính năng ACID.

### 3.2. Ánh Xạ Mô Hình Dim/Fact sang Bảng Iceberg
Quy chuẩn phân Dim/Fact và cấu trúc lưu trữ của hệ thống được quy định chi tiết tại [datalake_standard.md](file:///d:/project/stock-warehouse/utilities/datalake_standard.md):

| STT | Bảng nguồn thô | Tên bảng Iceberg đích | Thư mục lưu trữ đích trên MinIO | Phân vùng (Partitioning) | Chế độ ghi |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `company_overview` | `dim_company` | `stock_db.db/dim_company` | Không phân vùng | overwrite |
| 2 | `people` | `dim_owner` | `stock_db.db/dim_owner` | Không phân vùng | overwrite |
| 3 | `history_price` | `fact_history_price` | `stock_db.db/fact_history_price` | `years(prd_id)` (Phân vùng Năm) | append (Upsert) |
| 4 | `market_index` | `fact_market_index` | `stock_db.db/fact_market_index` | Không phân vùng | append (Upsert) |
| 5 | `bctc` | `fact_financial_reports` | `stock_db.db/fact_financial_reports` | Không phân vùng | append (Upsert) |
| 6 | `financial_ratio` | `fact_financial_ratios` | `stock_db.db/fact_financial_ratios` | Không phân vùng | append (Upsert) |
| 7 | `electric_board` | `fact_electric_board` | `stock_db.db/fact_electric_board` | `days(prd_id)` (Phân vùng Ngày) | append (Upsert) |
| 8 | `macro_economy` | `fact_macro_economy` | `stock_db.db/fact_macro_economy` | Không phân vùng | append (Upsert) |
| 9 | `vn_macro_yearly` | `fact_vn_macro_yearly` | `stock_db.db/fact_vn_macro_yearly` | Không phân vùng | append (Upsert) |
| 10 | `global_index` | `fact_global_index` | `stock_db.db/fact_global_index` | Không phân vùng | append (Upsert) |
| 11 | `news` | `fact_news` | `stock_db.db/fact_news` | `months(prd_id)` (Phân vùng Tháng) | append (Upsert) |

### 3.3. Quy Chuẩn Đồng Bộ & Làm Sạch Dữ Liệu
Để đồng nhất dữ liệu chuỗi thời gian phân tích, hệ thống tuân thủ các quy tắc nghiêm ngặt:
*   **Quy chuẩn tên cột thời gian (`prd_id`):** Tất cả các cột mốc thời gian đại diện cho phiên hoặc chu kỳ báo cáo đều bắt buộc phải đổi tên thành `prd_id` (ví dụ: `trading_date` $\rightarrow$ `prd_id`, `published` $\rightarrow$ `prd_id`, `ts` $\rightarrow$ `prd_id`). Các cột thời gian kiểm toán hệ thống được giữ nguyên tên (như `import_time`, `created_at`).
*   **Định dạng thời gian (Datetime Format):**
    *   Cột chỉ có ngày (Date-only): Định dạng bắt buộc `yyyy-mm-dd` (ví dụ: `2026-07-07`).
    *   Cột có giờ (Timestamp): Định dạng bắt buộc `yyyy-mm-dd hh:mm:ss` (ví dụ: `2026-07-07 09:00:00`).
*   **Cơ chế Idempotent Ghi dữ liệu:** Để ngăn chặn lỗi lặp dữ liệu (double-count) khi chạy lại (retry) các DAG Airflow, file xử lý [sync_landing_to_iceberg.py](file:///d:/project/stock-warehouse/data-pipeline/etl/spark/apps/minio_to_iceberg/sync_landing_to_iceberg.py) của Spark sẽ thực hiện việc quét dữ liệu nguồn trước, chạy lệnh `DELETE FROM [iceberg_table] WHERE prd_id IN (...)` (hoặc match theo cặp ticker/year/quarter) để xóa toàn bộ dữ liệu trùng của phiên/chu kỳ đó trước khi thực hiện `write` append dữ liệu mới.

### 3.4. Spark Command Server (Cơ chế Điều Phối Container)
Do chạy trong môi trường Container tách biệt, Airflow điều phối các task Spark bằng cách gửi các yêu cầu API qua REST tới dịch vụ [spark_cmd_server.py](file:///d:/project/stock-warehouse/data-pipeline/etl/spark/apps/server/spark_cmd_server.py) chạy trên container Spark Master (Lắng nghe tại Port `8088`).
*   `POST /submit`: Nhận lệnh chạy (ví dụ: `spark-submit ...`), sinh ngẫu nhiên một `task_id` và kích hoạt luồng chạy ngầm (subprocess thread).
*   `GET /status/<task_id>`: Cho phép Airflow gọi vòng lặp (poll) để kiểm tra trạng thái thực thi (`RUNNING`, `FINISHED`, `FAILED`) và lấy log đầu ra của job Spark.

---

## 4. Chi Tiết Cơ Sở Dữ Liệu (Database Schemas)

Hệ thống sử dụng mô hình cơ sở dữ liệu kép phục vụ cho hai tác vụ khác nhau:

### 4.1. CSDL Phục vụ Phân tích OLAP (ClickHouse - `stock_db`)
Toàn bộ bảng lưu trữ trong ClickHouse được tạo tự động thông qua các file SQL trong [clickhouse/init](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/):
1.  **Tích hợp Datalake qua Iceberg Engine:** Các bảng phân tích được ánh xạ trực tiếp tới thư mục lưu trữ metadata của Iceberg trên MinIO qua file [01_clickhouse_iceberg_setup.sql](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/01_clickhouse_iceberg_setup.sql):
    ```sql
    CREATE TABLE stock_db.fact_history_price
    ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_history_price', 'minioadmin', 'minioadmin');
    ```
    *Nhờ đó, khi Spark cập nhật bảng Iceberg trên MinIO, ClickHouse lập tức cập nhật dữ liệu mà không cần chạy job ETL đồng bộ nào khác.*
2.  **Tích hợp Real-time qua Kafka Engine:** Thiết lập trong file [02_clickhouse_kafka_setup.sql](file:///d:/project/stock-warehouse/data-pipeline/etl/clickhouse/init/02_clickhouse_kafka_setup.sql) gồm 3 phần:
    *   `stock_db.realtime_quotes_ch`: Bảng lưu trữ đích sử dụng **MergeTree Engine** tối ưu ghi/đọc sắp xếp theo `(ticker, prd_id)`.
    *   `stock_db.kafka_realtime_quotes_ch`: Bảng đệm sử dụng **Kafka Engine** kết nối trực tiếp tới Kafka Broker `kafka:9093`, topic `market.quotes.raw`.
    *   `stock_db.mv_kafka_to_realtime_quotes_ch`: Một **Materialized View** tự động trigger để đọc dữ liệu thô từ bảng đệm Kafka, thực hiện chuyển đổi kiểu dữ liệu (Timestamp millisecond sang DateTime64) rồi ghi thẳng xuống bảng đích `realtime_quotes_ch`.

### 4.2. CSDL Nghiệp vụ & BI Platform (PostgreSQL)
Cơ sở dữ liệu PostgreSQL (Xem chi tiết tại [postgres_schema.csv](file:///d:/project/stock-warehouse/utilities/postgres_schema.csv)) chứa 2 schema:

#### A. Schema Mặc định `public` (Dữ liệu DWH phục vụ BI & Chatbot)
Được định nghĩa cấu trúc trong [schema_v1.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/schema_v1.sql) chứa bản sao lưu dữ liệu tài chính phục vụ truy vấn OLTP và Text-to-SQL:
*   `history_price`: Giá đóng cửa, mở cửa, cao, thấp và khối lượng EOD của cổ phiếu. Khóa chính `(ticker, trading_date)`.
*   `market_index`: Điểm số lịch sử của chỉ số VNIndex, HNX, UPCOM.
*   `company_overview`: Hồ sơ thông tin doanh nghiệp, phân ngành ICB 3 cấp, sàn niêm yết.
*   `owner`: Danh sách cổ đông lớn, ban lãnh đạo và tỷ lệ sở hữu.
*   `bctc`: Lưu chi tiết các chỉ tiêu báo cáo tài chính thô của doanh nghiệp (`ind_code`, `value`, `report_code`). Khóa chính `(ticker, year, quarter, ind_code)`.
*   `financial_ratio`: Chỉ số tài chính định giá và hiệu quả hoạt động được tính sẵn (ROE, ROA, PE, PB, EPS, Market Cap...).
*   `electric_board`: Lưu snapshot bảng giá chốt ngày giao dịch.
*   `macro_economy`: Giá vàng, giá dầu thô, tỷ giá tiền tệ theo ngày.
*   `vn_macro_yearly`: Các chỉ số kinh tế vĩ mô Việt Nam theo năm.
*   `news`: Tin tức cào về kèm thông tin sắc thái cảm xúc (`sentiment`) và nhãn ngành (`icb_name`).
*   `event`: Lịch trình các sự kiện công ty.

#### B. Schema Quản trị & Tiện ích `system` (Lưu thông tin nghiệp vụ ứng dụng)
Định nghĩa trong [auth_tables.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/auth_tables.sql) và [schema_system.sql](file:///d:/project/stock-warehouse/bi-platform/BE/app/database/schema_system.sql):
*   `system.roles`: Vai trò người dùng (`user`, `admin`, `moderator`).
*   `system.users`: Thông tin tài khoản, mật khẩu băm, phương thức auth, secret khóa 2FA.
*   `system.refresh_tokens`: Token duy trì phiên đăng nhập, IP và thông tin thiết bị.
*   `system.password_reset_tokens`: Mã phục hồi mật khẩu.
*   `system.article_clicks`: Ghi nhận nhật ký người dùng click đọc tin tức.
*   `system.search_logs`: Ghi nhận từ khóa tìm kiếm tin tức của người dùng.
*   `system.stock_clicks`: Ghi nhận mã cổ phiếu người dùng bấm vào xem.
*   `system.stock_search_logs`: Ghi nhận từ khóa tìm kiếm mã chứng khoán.
*   `system.sidebar_clicks`: Thống kê tần suất click sử dụng các menu Sidebar của FE.
*   `system.login_logs`: Ghi log chi tiết lịch sử đăng nhập để phân tích bảo mật.
*   `system.session_logs`: Lưu vết chu kỳ phiên làm việc của người dùng dựa trên API Heartbeat gửi định kỳ mỗi 30 giây từ Frontend để đo lường thời gian online thực tế.
*   `system.stock_price_alerts`: Lưu thiết lập cảnh báo giá cổ phiếu của người dùng.
*   `system.user_favorite_stocks`: Lưu danh sách các mã cổ phiếu yêu thích (Watchlist) của người dùng.

---

## 5. Cấu Trúc Thư Mục Dự Án (Project Structure)

Dự án được phân chia mô-đun khoa học, tách biệt giữa luồng xử lý Big Data và nền tảng ứng dụng web:

```text
stock-warehouse/
├── bi-platform/                        # NỀN TẢNG BI (FRONTEND & BACKEND)
│   ├── BE/                             # Backend API (FastAPI)
│   │   ├── app/
│   │   │   ├── api/                    # Định nghĩa các router REST API chung
│   │   │   ├── core/                   # Cấu hình bảo mật, JWT, Middleware, biến môi trường
│   │   │   ├── database/               # File kết nối DB (Postgres & Clickhouse) và các script schema SQL
│   │   │   │   ├── auth_tables.sql     # Schema bảng phân quyền và đăng nhập
│   │   │   │   ├── schema_system.sql   # Schema bảng tracking và logs nghiệp vụ
│   │   │   │   └── schema_v1.sql       # Schema bảng dữ liệu tài chính
│   │   │   ├── modules/                # Thư mục chứa các module nghiệp vụ
│   │   │   │   ├── auth/               # Nghiệp vụ đăng ký, đăng nhập, 2FA
│   │   │   │   ├── stock/              # Phân tích cổ phiếu, định giá, định lượng
│   │   │   │   └── chatbot/            # Kiến trúc RAG Chatbot
│   │   │   │       ├── agents/         # Analyst, Data Retriever, Insight Agents
│   │   │   │       ├── retrieval/      # Tìm kiếm ngữ nghĩa trên Vector DB (FAISS)
│   │   │   │       ├── sql/            # Công cụ thực thi sinh câu lệnh SQL
│   │   │   │       └── system_prompt/  # File text cấu hình prompts cho các AI agents
│   │   │   ├── websocket/              # Quản lý Websocket truyền dữ liệu thời gian thực
│   │   │   └── main.py                 # File chạy chính của Backend API
│   │   ├── .env                        # Biến môi trường Backend
│   │   └── requirements.txt            # Thư viện Python Backend
│   ├── FE/                             # Frontend UI (Next.js App Router)
│   │   ├── app/                        # Các pages chính (App Router)
│   │   │   ├── (auth)/                 # Nhóm trang đăng nhập, đăng ký, 2FA
│   │   │   ├── market/                 # Trang bảng điện & tổng quan thị trường
│   │   │   ├── stock/                  # Trang phân tích chi tiết mã chứng khoán
│   │   │   ├── portfolio/              # Trang quản lý danh mục đầu tư
│   │   │   └── stockpilot/             # Trang giao diện trợ lý ảo AI
│   │   ├── components/                 # Các UI Components dùng chung (Button, Input, Charts...)
│   │   ├── lib/                        # Tiện ích chung, cấu hình Axios client
│   │   ├── hooks/                      # Các custom React hooks
│   │   ├── .env.local                  # Biến môi trường Frontend
│   │   └── package.json                # Quản lý dependencies thư viện Frontend
│   ├── docker-compose.yml              # Docker cấu hình riêng cho BI Platform
│   └── README.md                       # Tài liệu chi tiết nội bộ của BI Platform
│
├── data-pipeline/                      # HẠ TẦNG BIG DATA LAKEHOUSE & ETL
│   ├── etl/                            # Thư mục chính điều phối cụm ETL
│   │   ├── airflow/                    # Hạ tầng Apache Airflow
│   │   │   ├── Dockerfile              # Build image Airflow có cài các driver cần thiết
│   │   │   ├── dags/                   # 22 DAGs Airflow crawl, đồng bộ dữ liệu
│   │   │   ├── config/                 # Cấu hình Airflow
│   │   │   └── requirements.txt        # Các thư viện python chạy trong Airflow
│   │   ├── clickhouse/                 # Cấu hình Clickhouse
│   │   │   └── init/                   # Các script khởi tạo bảng Iceberg & Kafka
│   │   ├── spark/                      # Hạ tầng Apache Spark
│   │   │   ├── apps/                   # Các Spark Jobs xử lý dữ liệu thô sang Iceberg
│   │   │   │   ├── minio_to_iceberg/   # Spark batch sync Landing zone sang Iceberg
│   │   │   │   ├── kafka_to_iceberg/   # Spark streaming ghi dữ liệu Kafka sang Iceberg
│   │   │   │   ├── iceberg_to_clickhouse/# Spark sync thủ công Iceberg sang Clickhouse
│   │   │   │   └── server/             # Spark Command Server API
│   │   │   └── conf/                   # Cấu hình kết nối S3/MinIO cho Spark
│   │   ├── kafka/                      # Cấu hình cụm Message Broker Kafka
│   │   ├── minio/                      # Thư mục lưu dữ liệu MinIO cục bộ
│   │   ├── .env                        # Biến môi trường cụm ETL Datalake
│   │   └── docker-compose.yaml         # Docker compose khởi chạy toàn bộ cụm Datalakehouse
│
├── utilities/                          # TIỆN ÍCH & TÀI LIỆU QUY CHUẨN
│   ├── architecture.txt                # Sơ đồ tổng thể hạ tầng văn bản
│   ├── datalake_standard.md            # Quy chuẩn thiết kế Dim/Fact, prd_id, thời gian
│   └── postgres_schema.csv             # Bảng đặc tả chi tiết cấu trúc cột Postgres
│
├── docs/                               # TÀI LIỆU HÌNH ẢNH
│   └── images/                         # Sơ đồ kiến trúc luồng dữ liệu
│
├── docker-compose.yml                  # FILE COMPOSE ROOT (Gộp cả cụm ETL và BI bằng include)
└── .gitignore                          # Cấu hình bỏ qua tệp tin Git
```

---

## 6. Hướng Dẫn Cài Đặt & Khởi Chạy (Installation)

### 6.1. Khởi chạy toàn bộ hệ thống bằng Docker Compose (Khuyên dùng)
Hệ thống hỗ trợ cơ chế `include` của Docker Compose mới nhất, cho phép khởi chạy toàn bộ cả 2 phân hệ (Data Pipeline và BI Platform) chỉ bằng một câu lệnh duy nhất tại thư mục gốc của dự án.

#### Yêu cầu hệ thống:
*   Docker Desktop đã cài đặt trên máy.
*   Cấu hình RAM trống tối thiểu: **8GB** (do cụm Spark, Airflow, ClickHouse và Kafka tốn nhiều tài nguyên).

#### Các bước khởi chạy:
1.  **Sao chép cấu hình môi trường:**
    *   Di chuyển vào thư mục [data-pipeline/etl](file:///d:/project/stock-warehouse/data-pipeline/etl/) và tạo file `.env` từ file `.env.example`:
        ```bash
        cd data-pipeline/etl
        cp .env.example .env
        ```
    *   Di chuyển vào thư mục [bi-platform/BE](file:///d:/project/stock-warehouse/bi-platform/BE/) và tạo file `.env` từ file `.env.example`:
        ```bash
        cd ../../bi-platform/BE
        cp .env.example .env
        ```
    *   Di chuyển vào thư mục [bi-platform/FE](file:///d:/project/stock-warehouse/bi-platform/FE/) và tạo file `.env.local` từ file `.env.example`:
        ```bash
        cd ../FE
        cp .env.example .env.local
        ```
        *Lưu ý: Điền khóa `OPENAI_API_KEY` vào file `.env` của Backend để sử dụng tính năng Chatbot AI.*

2.  **Khởi chạy Docker Compose tại thư mục gốc:**
    Quay trở lại thư mục gốc của dự án và chạy:
    ```bash
    cd ../..
    docker compose up -d
    ```
    *Docker sẽ tự động tải các service và xây dựng môi trường cho cả Data Lakehouse lẫn Nền tảng BI.*

3.  **Kiểm tra các cổng dịch vụ đang chạy:**
    *   **Frontend UI:** `http://localhost:3000`
    *   **FastAPI Backend Swagger Docs:** `http://localhost:8000/docs`
    *   **Apache Airflow Webserver:** `http://localhost:8080` (Tài khoản mặc định: `airflow` / `airflow`)
    *   **MinIO Console (S3 Storage):** `http://localhost:9001` (Tài khoản mặc định: `minioadmin` / `minioadmin`)
    *   **ClickHouse Server:** `localhost:8123` (HTTP) và `9000` (Native client)

---

### 6.2. Cài đặt thủ công từng phần để Phát triển (Development Mode)
Nếu bạn muốn sửa đổi mã nguồn và chạy các dịch vụ dưới local (không dùng Docker cho FE/BE):

#### Bước 1: Khởi chạy các cơ sở dữ liệu và cụm hạ tầng
Di chuyển vào thư mục [data-pipeline/etl](file:///d:/project/stock-warehouse/data-pipeline/etl/) để khởi chạy các Container của MinIO, Spark, ClickHouse, Postgres, Kafka và Airflow:
```bash
cd data-pipeline/etl
docker compose up -d
```

#### Bước 2: Cài đặt và Chạy Backend API (FastAPI)
1.  Di chuyển vào thư mục Backend:
    ```bash
    cd ../../bi-platform/BE
    ```
2.  Tạo và kích hoạt môi trường ảo Python (Virtual Environment):
    ```bash
    python -m venv .venv
    # Kích hoạt trên Windows:
    .venv\Scripts\activate
    # Kích hoạt trên macOS/Linux:
    source .venv/bin/activate
    ```
3.  Cài đặt các thư viện phụ thuộc:
    ```bash
    pip install -r requirements.txt
    ```
4.  Cấu hình cơ sở dữ liệu trong file `.env` trỏ về các cổng tương ứng của Docker đã khởi chạy (ví dụ `localhost:5432` cho PostgreSQL, `localhost:8123` cho ClickHouse).
5.  Khởi chạy Backend với Uvicorn:
    ```bash
    uvicorn app.main:app --reload
    ```
    *Backend sẽ chạy tại địa chỉ: `http://localhost:8000`*

#### Bước 3: Cài đặt và Chạy Frontend (Next.js)
1.  Mở một Terminal mới và di chuyển vào thư mục Frontend:
    ```bash
    cd bi-platform/FE
    ```
2.  Cài đặt các package Node.js:
    ```bash
    npm install
    ```
3.  Kiểm tra file `.env.local` đã trỏ đúng URL API của Backend:
    ```text
    NEXT_PUBLIC_API_URL=http://localhost:8000
    ```
4.  Chạy Frontend ở chế độ phát triển:
    ```bash
    npm run dev
    ```
    *Frontend sẽ chạy tại địa chỉ: `http://localhost:3000`*
