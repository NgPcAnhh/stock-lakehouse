CREATE DATABASE IF NOT EXISTS stock_db;

DROP TABLE IF EXISTS stock_db.dim_company;
CREATE TABLE stock_db.dim_company
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/dim_company', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.dim_owner;
CREATE TABLE stock_db.dim_owner
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/dim_owner', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_history_price;
CREATE TABLE stock_db.fact_history_price
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_history_price', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_market_index;
CREATE TABLE stock_db.fact_market_index
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_market_index', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_financial_reports;
CREATE TABLE stock_db.fact_financial_reports
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_financial_reports', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_financial_ratios;
CREATE TABLE stock_db.fact_financial_ratios
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_financial_ratios', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_electric_board;
CREATE TABLE stock_db.fact_electric_board
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_electric_board', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_macro_economy;
CREATE TABLE stock_db.fact_macro_economy
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_macro_economy', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_vn_macro_yearly;
CREATE TABLE stock_db.fact_vn_macro_yearly
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_vn_macro_yearly', 'minioadmin', 'minioadmin');

DROP TABLE IF EXISTS stock_db.fact_news;
CREATE TABLE stock_db.fact_news
ENGINE = Iceberg('http://minio:9000/stock-datalake/stock_db/fact_news', 'minioadmin', 'minioadmin');
