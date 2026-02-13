import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# Import Operator chuẩn cho SQL (thay thế PostgresOperator cũ)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# --- XỬ LÝ ĐƯỜNG DẪN (PATH FIX) ---
# Mục đích: Giúp Airflow tìm thấy folder 'scripts' nằm cùng cấp với folder 'dags'

# 1. Lấy đường dẫn tuyệt đối của file DAG hiện tại
current_dag_path = os.path.abspath(__file__)
# 2. Lấy folder chứa DAG (folder 'dags')
dags_folder = os.path.dirname(current_dag_path)
# 3. Lấy folder dự án gốc (airflow_booking_etl) - Là cha của folder dags
project_root = os.path.dirname(dags_folder)

# *FIX ĐẶC BIỆT*: Nếu bạn lỡ để file trong dags/dags/ (lồng nhau)
# Đoạn này sẽ kiểm tra xem folder scripts có ở đó không, nếu không thì lùi thêm 1 cấp nữa
if not os.path.exists(os.path.join(project_root, 'scripts')):
    print(f"Không thấy scripts tại {project_root}, thử lùi thêm 1 cấp...")
    project_root = os.path.dirname(project_root)

# 4. Thêm vào biến môi trường hệ thống
if project_root not in sys.path:
    sys.path.append(project_root)

print(f"DEBUG: Project Root được thêm vào path: {project_root}")

# --- IMPORT HÀM CRAWL ---
# Bỏ try/except để nếu lỗi thì báo ngay tại đây (ModuleNotFoundError)
from scripts.booking_crawler import run_crawler_and_load_staging

default_args = {
    'owner': 'kietto03',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='booking_etl_pipeline_final', # Đổi tên ID để tránh cache
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule='30 8 * * *', 
    catchup=False,
    tags=['booking', 'etl']
) as dag:

    # TASK 1: CRAWL DATA
    task_crawl = PythonOperator(
        task_id='crawl_data_task',
        python_callable=run_crawler_and_load_staging
    )

    # TASK 2: SYNC DATABASE
    task_sync_db = SQLExecuteQueryOperator(
        task_id='sync_db_logic_task',
        conn_id='postgres_localhost',
        sql="CALL sync_booking_data();",
        autocommit=True
    )

    task_crawl >> task_sync_db