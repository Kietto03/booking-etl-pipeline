from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta
# Import hàm crawl của bạn
# from scripts.booking_crawler import run_crawler_and_load_staging 

default_args = {
    'owner': 'user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='booking_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval='30 8 * * *', # 8h30 sáng hàng ngày
    catchup=False
) as dag:

    # Task 1: Chạy Python Crawl -> Đẩy vào Staging Table
    task_crawl = PythonOperator(
        task_id='crawl_booking_data',
        python_callable=run_crawler_and_load_staging # Hàm này bạn viết ở phần 3
    )

    # Task 2: Chạy Procedure SQL để xử lý Logic Available/Status
    task_sync_db = SQLExecuteQueryOperator(
        task_id='sync_db_logic_task',
        conn_id='postgres_localhost',  # <--- Lưu ý: tên tham số là 'conn_id'
        sql="CALL sync_booking_data();",
        autocommit=True
    )

    # Flow
    task_crawl >> task_sync_db