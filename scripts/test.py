import sqlalchemy
from sqlalchemy import create_engine, text
import sys

# --- CẤU HÌNH (Bạn hãy điền y hệt như trong file crawler) ---
DB_USER = 'postgres'
DB_PASS = 'kiet'  # Thay password của bạn
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'booking_db'

def check_connection():
    print("-" * 50)
    print("🛠  ĐANG KIỂM TRA KẾT NỐI DATABASE...")
    print(f"📡 Host: {DB_HOST}:{DB_PORT} | DB: {DB_NAME} | User: {DB_USER}")
    print("-" * 50)

    connection_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    
    try:
        # 1. Thử tạo Engine
        engine = create_engine(connection_string)
        
        # 2. Thử kết nối thực tế
        with engine.connect() as conn:
            print("✅ KẾT NỐI THÀNH CÔNG! (Login OK)")

            # 3. Kiểm tra xem bảng 'booking_staging' có tồn tại không
            try:
                query = text("SELECT COUNT(*) FROM booking_staging;")
                result = conn.execute(query).scalar()
                print(f"✅ BẢNG 'booking_staging' ĐÃ TỒN TẠI.")
                print(f"📊 SỐ LƯỢNG BẢN GHI HIỆN TẠI: {result}")
                
                if result == 0:
                    print("⚠️ CẢNH BÁO: Bảng tồn tại nhưng KHÔNG CÓ DỮ LIỆU (Empty).")
                    print("   -> Lỗi nằm ở phần Crawler (không lấy được data) chứ không phải do DB.")
                else:
                    print("🎉 Tốt! Database đang chứa dữ liệu.")
                    
            except sqlalchemy.exc.ProgrammingError:
                print("❌ LỖI: Bảng 'booking_staging' CHƯA TỒN TẠI.")
                print("   -> Bạn cần chạy câu lệnh SQL 'CREATE TABLE...' trong DBeaver/PgAdmin trước.")
                
    except Exception as e:
        print("❌ KẾT NỐI THẤT BẠI!")
        print(f"Chi tiết lỗi: {e}")
        print("\n💡 Gợi ý sửa lỗi:")
        print("   1. Kiểm tra lại Mật khẩu (DB_PASS).")
        print("   2. Kiểm tra xem PostgreSQL có đang chạy không (Port 5432).")
        print("   3. Kiểm tra tên Database (booking_db) đã được tạo chưa.")

if __name__ == "__main__":
    check_connection()