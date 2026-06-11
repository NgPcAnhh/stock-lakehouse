from datetime import datetime, timedelta
import requests
import time
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from lake_to_dwh.utils import get_latest_partition, get_all_partitions

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
    # Resolve script path based on reorganization
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
    dag_id="minio_to_iceberg_sync",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 17 * * *",  # Run daily at 17:00 (5:00 PM)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["sync", "minio", "iceberg", "etl", "datalake"],
    description="Synchronize all raw data from MinIO landing to Iceberg catalog",
)
def minio_to_iceberg_sync():
    
    @task(task_id="sync_bctc")
    def task_sync_bctc():
        partitions = get_all_partitions(MINIO_BUCKET, "bctc_luong2/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for BCTC"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_financial_reports",
                source_path=source_path,
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} BCTC partition(s)"
        
    @task(task_id="sync_daily_price")
    def task_sync_daily_price():
        partitions = get_all_partitions(MINIO_BUCKET, "daily_price/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for daily price"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_history_price",
                source_path=source_path,
                time_col="trading_date",
                time_type="date",
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} daily price partition(s)"
        
    @task(task_id="sync_financial_ratio")
    def task_sync_financial_ratio():
        partitions = get_all_partitions(MINIO_BUCKET, "fin_ratio_v2/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for financial ratio"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_financial_ratios",
                source_path=source_path,
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} financial ratio partition(s)"
        
    @task(task_id="sync_index_price")
    def task_sync_index_price():
        partitions = get_all_partitions(MINIO_BUCKET, "index_price/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for index price"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_market_index",
                source_path=source_path,
                time_col="trading_date",
                time_type="date",
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} index price partition(s)"
        
    @task(task_id="sync_overview")
    def task_sync_overview():
        latest_partition = get_latest_partition(MINIO_BUCKET, "overview/", MINIO_CONN_ID)
        if not latest_partition:
            return "❌ No partition found for overview"
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
            return "❌ No partition found for people"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_landing_to_iceberg.py",
            target_table="dim_owner",
            source_path=source_path,
            mode="overwrite"
        )
        
    @task(task_id="sync_electric_board")
    def task_sync_electric_board():
        partitions = get_all_partitions(MINIO_BUCKET, "electric_board_per_day/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for electric board"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_electric_board",
                source_path=source_path,
                time_col="trading_date",
                time_type="date",
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} electric board partition(s)"
        
    @task(task_id="sync_news")
    def task_sync_news():
        partitions = get_all_partitions(MINIO_BUCKET, "news/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for news"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_news",
                source_path=source_path,
                time_col="published",
                time_type="timestamp",
                mode="append"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} news partition(s)"
        
    @task(task_id="sync_vn_macro_yearly")
    def task_sync_vn_macro_yearly():
        latest_partition = get_latest_partition(MINIO_BUCKET, "vn_macro_yearly/", MINIO_CONN_ID)
        if not latest_partition:
            return "❌ No partition found for VN macro yearly"
        source_path = f"s3a://{MINIO_BUCKET}/{latest_partition}"
        return submit_spark_job(
            script_name="sync_vn_macro_yearly_to_iceberg.py",
            target_table="fact_vn_macro_yearly",
            source_path=source_path
        )

    # === Global Index Subfolders ===
    @task(task_id="sync_global_index_usd_vnd")
    def task_sync_global_index_usd_vnd():
        partitions = get_all_partitions(MINIO_BUCKET, "global_index/usd_vnd/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=USD_VND")
        return f"✅ Synced {len(partitions)} usd_vnd partition(s)"

    @task(task_id="sync_global_index_dxy")
    def task_sync_global_index_dxy():
        partitions = get_all_partitions(MINIO_BUCKET, "global_index/dxy_index/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=DXY")
        return f"✅ Synced {len(partitions)} dxy partition(s)"

    @task(task_id="sync_global_index_usd_cny")
    def task_sync_global_index_usd_cny():
        partitions = get_all_partitions(MINIO_BUCKET, "global_index/usd_cny/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=USD_CNY")
        return f"✅ Synced {len(partitions)} usd_cny partition(s)"

    @task(task_id="sync_global_index_eur_usd")
    def task_sync_global_index_eur_usd():
        partitions = get_all_partitions(MINIO_BUCKET, "global_index/eur_usd/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=EUR_USD")
        return f"✅ Synced {len(partitions)} eur_usd partition(s)"

    @task(task_id="sync_global_index_us_bond_10y")
    def task_sync_global_index_us_bond_10y():
        partitions = get_all_partitions(MINIO_BUCKET, "global_index/us_bond_10y/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=US_BOND_10Y")
        return f"✅ Synced {len(partitions)} us_bond_10y partition(s)"

    # === Macro Economy Subfolders ===
    @task(task_id="sync_macro_economy_xau")
    def task_sync_macro_economy_xau():
        partitions = get_all_partitions(MINIO_BUCKET, "macro_economy/xau/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=XAU")
        return f"✅ Synced {len(partitions)} xau partition(s)"

    @task(task_id="sync_macro_economy_oil")
    def task_sync_macro_economy_oil():
        partitions = get_all_partitions(MINIO_BUCKET, "macro_economy/oil/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=OIL")
        return f"✅ Synced {len(partitions)} oil partition(s)"

    @task(task_id="sync_macro_economy_dji")
    def task_sync_macro_economy_dji():
        partitions = get_all_partitions(MINIO_BUCKET, "macro_economy/dowjone/", MINIO_CONN_ID)
        if not partitions: return "❌ No partition"
        for partition in partitions:
            submit_spark_job("sync_landing_to_iceberg.py", "fact_macro_economy", f"s3a://{MINIO_BUCKET}/{partition}", "date", "date", "append", "asset_type=DJI")
        return f"✅ Synced {len(partitions)} dji partition(s)"

    # === Realtime Quotes ===
    @task(task_id="sync_realtime_quotes")
    def task_sync_realtime_quotes():
        partitions = get_all_partitions(MINIO_BUCKET, "realtime/", MINIO_CONN_ID)
        if not partitions:
            return "❌ No partition found for realtime quotes"
        results = []
        for partition in partitions:
            source_path = f"s3a://{MINIO_BUCKET}/{partition}"
            res = submit_spark_job(
                script_name="sync_landing_to_iceberg.py",
                target_table="fact_realtime_quotes",
                source_path=source_path,
                time_col="ts",
                time_type="timestamp",
                mode="append",
                file_format="parquet"
            )
            results.append(res)
        return f"✅ Synced {len(partitions)} realtime quotes partition(s)"

    # Summary Report task
    @task(task_id="summary_report")
    def task_summary_report(results: list):
        logger.info("=" * 70)
        logger.info("📊 MINIO TO ICEBERG DATALAKE SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        for idx, result in enumerate(results):
            logger.info(f"{idx + 1}. {result}")
        logger.info("=" * 70)
        success_count = sum(1 for r in results if r and '✅' in str(r))
        logger.info(f"✅ Successful: {success_count}/{len(results)}")
        if success_count < len(results):
            logger.warning(f"⚠️ Failed: {len(results) - success_count}/{len(results)}")
        return f"Completed: {success_count}/{len(results)} successful"

    # Define tasks execution
    results = [
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
    
    task_summary_report(results)

# Instantiate the DAG
minio_to_iceberg_sync()
