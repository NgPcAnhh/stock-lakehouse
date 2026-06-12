from datetime import datetime, timedelta
import requests
import time
import logging
import base64
import json
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

def submit_spark_job(tasks: list):
    # Construct base64 config
    config_dict = {"tasks": tasks}
    config_json = json.dumps(config_dict)
    config_base64 = base64.b64encode(config_json.encode('utf-8')).decode('utf-8')
    
    script_path = "/opt/spark/apps/minio_to_iceberg/sync_all_landing_to_iceberg.py"

    cmd = [
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4",
        "--master", "spark://spark-master:7077",
        "--executor-memory", "1500m",
        "--driver-memory", "1500m",
        script_path,
        "--base64_config", config_base64
    ]
            
    cmd_str = " ".join(cmd)
    logger.info("Submitting unified Spark job to sync all latest partitions...")
    
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
                return "✅ Success: All latest sync tasks executed."
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
    description="Incremental sync from MinIO Landing to Iceberg and auto-reflected in ClickHouse (Unified Spark Session)",
)
def spark_process_dag():
    
    @task(task_id="sync_all_to_iceberg")
    def task_sync_all_to_iceberg():
        tasks = []
        
        # 1. bctc
        latest = get_latest_partition(MINIO_BUCKET, "bctc_luong2/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_financial_reports",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 2. daily_price
        latest = get_latest_partition(MINIO_BUCKET, "daily_price/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_history_price",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "time_col": "trading_date",
                "time_type": "date",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 3. financial_ratio
        latest = get_latest_partition(MINIO_BUCKET, "fin_ratio_v2/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_financial_ratios",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 4. index_price
        latest = get_latest_partition(MINIO_BUCKET, "index_price/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_market_index",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "time_col": "trading_date",
                "time_type": "date",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 5. overview
        latest = get_latest_partition(MINIO_BUCKET, "overview/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "dim_company",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "mode": "overwrite",
                "script_type": "standard"
            })
            
        # 6. people
        latest = get_latest_partition(MINIO_BUCKET, "people/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "dim_owner",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "mode": "overwrite",
                "script_type": "standard"
            })
            
        # 7. electric_board
        latest = get_latest_partition(MINIO_BUCKET, "electric_board_per_day/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_electric_board",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "time_col": "trading_date",
                "time_type": "date",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 8. news
        latest = get_latest_partition(MINIO_BUCKET, "news/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_news",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "time_col": "published",
                "time_type": "timestamp",
                "mode": "append",
                "script_type": "standard"
            })
            
        # 9. vn_macro_yearly
        latest = get_latest_partition(MINIO_BUCKET, "vn_macro_yearly/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_vn_macro_yearly",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "script_type": "vn_macro_yearly"
            })
            
        # === Global Index Subfolders ===
        global_indexes = [
            ("global_index/usd_vnd/", "USD_VND"),
            ("global_index/dxy_index/", "DXY"),
            ("global_index/usd_cny/", "USD_CNY"),
            ("global_index/eur_usd/", "EUR_USD"),
            ("global_index/us_bond_10y/", "US_BOND_10Y")
        ]
        for prefix, asset_type in global_indexes:
            latest = get_latest_partition(MINIO_BUCKET, prefix, MINIO_CONN_ID)
            if latest:
                tasks.append({
                    "target": "fact_macro_economy",
                    "source": f"s3a://{MINIO_BUCKET}/{latest}",
                    "time_col": "date",
                    "time_type": "date",
                    "mode": "append",
                    "static": f"asset_type={asset_type}",
                    "script_type": "standard"
                })
                
        # === Macro Economy Subfolders ===
        macro_economies = [
            ("macro_economy/xau/", "XAU"),
            ("macro_economy/oil/", "OIL"),
            ("macro_economy/dowjone/", "DJI")
        ]
        for prefix, asset_type in macro_economies:
            latest = get_latest_partition(MINIO_BUCKET, prefix, MINIO_CONN_ID)
            if latest:
                tasks.append({
                    "target": "fact_macro_economy",
                    "source": f"s3a://{MINIO_BUCKET}/{latest}",
                    "time_col": "date",
                    "time_type": "date",
                    "mode": "append",
                    "static": f"asset_type={asset_type}",
                    "script_type": "standard"
                })
                
        # === Realtime Quotes ===
        latest = get_latest_partition(MINIO_BUCKET, "realtime/", MINIO_CONN_ID)
        if latest:
            tasks.append({
                "target": "fact_realtime_quotes",
                "source": f"s3a://{MINIO_BUCKET}/{latest}",
                "time_col": "ts",
                "time_type": "timestamp",
                "mode": "append",
                "format": "parquet",
                "script_type": "standard"
            })
            
        if not tasks:
            logger.info("⚠️ No new partitions found to sync.")
            return "No partitions found"
            
        logger.info(f"🚀 Submitting {len(tasks)} sync tasks to Spark...")
        return submit_spark_job(tasks)

    # === ClickHouse Validation Task ===
    @task(task_id="verify_clickhouse_data")
    def task_verify_clickhouse_data(sync_results: str):
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
    def task_summary_report(sync_results: str, verification_report: list):
        logger.info("=" * 70)
        logger.info("📊 SPARK PROCESS DATALAKE SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        logger.info(f"Sync Result: {sync_results}")
        logger.info("--- ClickHouse Integration Check ---")
        for report in verification_report:
            logger.info(report)
        logger.info("=" * 70)
        return "Completed"

    # Define tasks execution
    sync_res = task_sync_all_to_iceberg()
    verification = task_verify_clickhouse_data(sync_res)
    task_summary_report(sync_res, verification)

# Instantiate the DAG
spark_process_dag()
