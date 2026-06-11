from datetime import datetime, timedelta
import requests
import time
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from lake_to_dwh.utils import get_latest_partition

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc"
)

MINIO_CONN_ID = "minio_finance"

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def submit_spark_job(
    script_name: str,
    target_table: str,
    source_path: str,
    time_col: str = None,
    time_type: str = None,
    mode: str = "append",
    static: str = None,
    file_format: str = "csv"
):
    # Construct the spark-submit command
    script_path = f"/opt/spark/apps/minio_to_iceberg/{script_name}"

    cmd = [
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4",
        "--master", "spark://spark-master:7077",
        "--executor-memory", "1500m",
        "--driver-memory", "1500m",
        script_path,
        "--source", source_path
    ]
    
    if script_name == "sync_landing_to_iceberg.py":
        cmd.extend(["--target", target_table])
        cmd.extend(["--mode", mode])
        cmd.extend(["--format", file_format])
        if time_col:
            cmd.extend(["--time_col", time_col])
        if time_type:
            cmd.extend(["--time_type", time_type])
        if static:
            cmd.extend(["--static", static])
            
    cmd_str = " ".join(cmd)
    logger.info(f"Submitting Spark job: {cmd_str}")
    
    # POST to spark_cmd_server
    try:
        res = requests.post("http://spark-master:8088/submit", json={"command": cmd_str}, timeout=30)
        if res.status_code != 202:
            raise AirflowException(f"Failed to submit spark job to command server: {res.text}")
        
        task_id = res.json().get("task_id")
        logger.info(f"Spark job submitted successfully. Task ID: {task_id}")
    except Exception as e:
        raise AirflowException(f"Error connecting to spark command server: {str(e)}")
    
    # Poll status
    while True:
        time.sleep(5)
        try:
            status_res = requests.get(f"http://spark-master:8088/status/{task_id}", timeout=30)
            if status_res.status_code != 200:
                logger.warning(f"Failed to fetch task status for {task_id}")
                continue
            
            task_info = status_res.json()
            status = task_info.get("status")
            if status == "FINISHED":
                logger.info("✅ Spark job finished successfully!")
                logger.info("--- Spark Job Logs ---")
                logger.info(task_info.get("log", ""))
                return f"✅ Success: {target_table} ({source_path})"
            elif status == "FAILED":
                logger.error("❌ Spark job failed!")
                logger.error("--- Spark Job Logs ---")
                logger.error(task_info.get("log", ""))
                raise AirflowException(f"Spark job failed with exit code {task_info.get('exit_code')}")
            else:
                logger.info("Spark job is still running...")
        except AirflowException:
            raise
        except Exception as e:
            logger.warning(f"Error polling task status: {str(e)}")

# ============================================================================

@dag(
    dag_id="spark_process",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 17 * * *",  # Run daily at 17:00 (5:00 PM)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["sync", "minio", "iceberg", "clickhouse", "etl", "datalake"],
    description="Incremental sync from MinIO Landing to Iceberg and auto-reflected in ClickHouse",
)
def spark_process_dag():
    
    @task(task_id="sync_bctc")
    def task_sync_bctc():
        latest_partition = get_latest_partition(MINIO_BUCKET, "bctc_luong2/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for BCTC"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_financial_reports",
            source_path=source_path,
            mode="append"
        )
        
    @task(task_id="sync_daily_price")
    def task_sync_daily_price():
        latest_partition = get_latest_partition(MINIO_BUCKET, "daily_price/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for daily price"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_history_price",
            source_path=source_path,
            time_col="trading_date",
            time_type="date",
            mode="append"
        )
        
    @task(task_id="sync_financial_ratio")
    def task_sync_financial_ratio():
        latest_partition = get_latest_partition(MINIO_BUCKET, "fin_ratio_v2/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for financial ratio"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_financial_ratios",
            source_path=source_path,
            mode="append"
        )
        
    @task(task_id="sync_index_price")
    def task_sync_index_price():
        latest_partition = get_latest_partition(MINIO_BUCKET, "index_price/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for index price"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_market_index",
            source_path=source_path,
            time_col="trading_date",
            time_type="date",
            mode="append"
        )
        
    @task(task_id="sync_overview")
    def task_sync_overview():
        latest_partition = get_latest_partition(MINIO_BUCKET, "overview/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for overview"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="dim_company",
            source_path=source_path,
            mode="overwrite"
        )
        
    @task(task_id="sync_people")
    def task_sync_people():
        latest_partition = get_latest_partition(MINIO_BUCKET, "people/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for people"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="dim_owner",
            source_path=source_path,
            mode="overwrite"
        )
        
    @task(task_id="sync_electric_board")
    def task_sync_electric_board():
        latest_partition = get_latest_partition(MINIO_BUCKET, "electric_board_per_day/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for electric board"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_electric_board",
            source_path=source_path,
            time_col="trading_date",
            time_type="date",
            mode="append"
        )
        
    @task(task_id="sync_news")
    def task_sync_news():
        latest_partition = get_latest_partition(MINIO_BUCKET, "news/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for news"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_news",
            source_path=source_path,
            time_col="published",
            time_type="timestamp",
            mode="append"
        )
        
    @task(task_id="sync_vn_macro_yearly")
    def task_sync_vn_macro_yearly():
        latest_partition = get_latest_partition(MINIO_BUCKET, "vn_macro_yearly/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for VN macro yearly"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_vn_macro_yearly_to_iceberg.py",
            target_table="fact_vn_macro_yearly",
            source_path=source_path
        )

    # === Global Index Subfolders ===
    @task(task_id="sync_global_index_usd_vnd")
    def task_sync_global_index_usd_vnd():
        latest_partition = get_latest_partition(MINIO_BUCKET, "global_index/usd_vnd/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=USD_VND")

    @task(task_id="sync_global_index_dxy")
    def task_sync_global_index_dxy():
        latest_partition = get_latest_partition(MINIO_BUCKET, "global_index/dxy_index/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=DXY")

    @task(task_id="sync_global_index_usd_cny")
    def task_sync_global_index_usd_cny():
        latest_partition = get_latest_partition(MINIO_BUCKET, "global_index/usd_cny/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=USD_CNY")

    @task(task_id="sync_global_index_eur_usd")
    def task_sync_global_index_eur_usd():
        latest_partition = get_latest_partition(MINIO_BUCKET, "global_index/eur_usd/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=EUR_USD")

    @task(task_id="sync_global_index_us_bond_10y")
    def task_sync_global_index_us_bond_10y():
        latest_partition = get_latest_partition(MINIO_BUCKET, "global_index/us_bond_10y/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=US_BOND_10Y")

    # === Macro Economy Subfolders ===
    @task(task_id="sync_macro_economy_xau")
    def task_sync_macro_economy_xau():
        latest_partition = get_latest_partition(MINIO_BUCKET, "macro_economy/xau/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=XAU")

    @task(task_id="sync_macro_economy_oil")
    def task_sync_macro_economy_oil():
        latest_partition = get_latest_partition(MINIO_BUCKET, "macro_economy/oil/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=OIL")

    @task(task_id="sync_macro_economy_dji")
    def task_sync_macro_economy_dji():
        latest_partition = get_latest_partition(MINIO_BUCKET, "macro_economy/dowjone/", MINIO_CONN_ID)
        if not latest_partition: return "⚠️ No partition"
        return submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{latest_partition}", "date", "date", "append", "asset_type=DJI")

    # === Realtime Quotes ===
    @task(task_id="sync_realtime_quotes")
    def task_sync_realtime_quotes():
        latest_partition = get_latest_partition(MINIO_BUCKET, "realtime/", MINIO_CONN_ID)
        if not latest_partition:
            return "⚠️ No partition found for realtime quotes"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="fact_realtime_quotes",
            source_path=source_path,
            time_col="ts",
            time_type="timestamp",
            mode="append",
            file_format="parquet"
        )

    # === ClickHouse Validation Task ===
    @task(task_id="verify_clickhouse_data")
    def task_verify_clickhouse_data(sync_results: list):
        logger.info("=" * 70)
        logger.info("📊 CHECKING CLICKHOUSE INTEGRATION (ICEBERG ENGINE)")
        logger.info("=" * 70)
        
        tables = [
            "fact_financial_reports",
            "fact_history_price",
            "fact_financial_ratios",
            "fact_market_index",
            "dim_company",
            "dim_owner",
            "fact_electric_board",
            "fact_news",
            "fact_realtime_quotes",
            "fact_macro_economy",
            "fact_vn_macro_yearly"
        ]
        
        verification_report = []
        for table in tables:
            try:
                # Query ClickHouse REST HTTP endpoint to count rows in the Iceberg-engine tables
                res = requests.post(
                    "http://clickhouse:8123/",
                    data=f"SELECT count() FROM stock_db.{table}",
                    headers={
                        "X-ClickHouse-User": "default",
                        "X-ClickHouse-Key": "default"
                    },
                    timeout=15
                )
                if res.status_code == 200:
                    row_count = res.text.strip()
                    logger.info(f"✓ ClickHouse stock_db.{table}: {row_count} rows")
                    verification_report.append(f"✓ {table}: {row_count} rows")
                else:
                    logger.warning(f"⚠️ ClickHouse stock_db.{table} query returned status {res.status_code}: {res.text}")
                    verification_report.append(f"⚠️ {table}: Failed to query ({res.text[:100]})")
            except Exception as e:
                logger.error(f"❌ ClickHouse stock_db.{table} connection error: {str(e)}")
                verification_report.append(f"❌ {table}: Error ({str(e)})")
                
        logger.info("=" * 70)
        return verification_report

    # Summary Report task
    @task(task_id="summary_report")
    def task_summary_report(sync_results: list, verification_report: list):
        logger.info("=" * 70)
        logger.info("📊 SPARK PROCESS DATALAKE SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        logger.info("--- Spark Iceberg Syncs ---")
        for idx, result in enumerate(sync_results):
            logger.info(f"{idx + 1}. {result}")
        logger.info("--- ClickHouse Integration Check ---")
        for report in verification_report:
            logger.info(report)
        logger.info("=" * 70)
        return "Completed"

    # Define tasks execution
    sync_results = [
        task_sync_bctc(),
        task_sync_daily_price(),
        task_sync_financial_ratio(),
        task_sync_index_price(),
        task_sync_overview(),
        task_sync_people(),
        task_sync_electric_board(),
        task_sync_news(),
        task_sync_vn_macro_yearly(),
        task_sync_global_index_usd_vnd(),
        task_sync_global_index_dxy(),
        task_sync_global_index_usd_cny(),
        task_sync_global_index_eur_usd(),
        task_sync_global_index_us_bond_10y(),
        task_sync_macro_economy_xau(),
        task_sync_macro_economy_oil(),
        task_sync_macro_economy_dji(),
        task_sync_realtime_quotes(),
    ]
    
    verification = task_verify_clickhouse_data(sync_results)
    task_summary_report(sync_results, verification)

# Instantiate the DAG
spark_process_dag()
