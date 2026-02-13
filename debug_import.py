import sys
import os

print("--- THÔNG TIN DEBUG ---")
print(f"1. Python đang chạy tại: {sys.executable}")
print(f"2. Phiên bản Python: {sys.version}")
print("\n3. Đang thử import PostgresOperator...")

try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    print("✅ THÀNH CÔNG! Thư viện đã được cài đặt đúng chỗ.")
except ImportError as e:
    print(f"❌ THẤT BẠI! Lỗi: {e}")
    print("\n--- GỢI Ý KHẮC PHỤC ---")
    print("Bạn đang chạy Python ở đường dẫn trên (mục 1).")
    print("Hãy copy dòng lệnh dưới đây và chạy trong Terminal để cài đúng vào Python này:")
    print(f"   {sys.executable} -m pip install apache-airflow-providers-postgres")