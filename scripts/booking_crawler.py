import pandas as pd
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
import time
import random
from sqlalchemy import create_engine, text

# --- CẤU HÌNH DATABASE ---
# Bạn thay đổi thông tin này cho đúng với máy của bạn
DB_USER = 'postgres'
DB_PASS = 'kiet'  # Thay password của bạn
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'booking_db'

# --- CẤU HÌNH ---
LIMIT_DAYS = 3       
LIMIT_GROUPS = 2     
LIMIT_ITEMS = 5      
HEADLESS_MODE = False  # <--- SỬA THÀNH FALSE ĐỂ HIỆN TRÌNH DUYỆT

def generate_url(city, check_in_date, group_config):
    check_in_str = check_in_date.strftime("%Y-%m-%d")
    check_out_str = (check_in_date + timedelta(days=1)).strftime("%Y-%m-%d")
    city_encoded = city.replace(" ", "+") 
    base_url = f"https://www.booking.com/searchresults.vi.html?ss={city_encoded}&checkin={check_in_str}&checkout={check_out_str}&group_adults={group_config['a']}&group_children={group_config['c']}&no_rooms={group_config['r']}"
    return base_url

def crawl_data():
    cities = ["Da Nang", "Hue"]
    
    all_groups = [
        {'id': '1_adult', 'a': 1, 'c': 0, 'r': 1}, 
        {'id': '2_adults', 'a': 2, 'c': 0, 'r': 1},
        {'id': '2_adults_1_child', 'a': 2, 'c': 1, 'r': 1},
        {'id': '2_adults_2_children', 'a': 2, 'c': 2, 'r': 2},
    ]
    groups = all_groups[:LIMIT_GROUPS] 
    
    start_date = datetime(2025, 6, 20)
    date_list = [start_date + timedelta(days=x) for x in range(LIMIT_DAYS)]
    
    all_results = []
    print(f"🚀 Bắt đầu Crawl (Hiện browser để debug)...")

    with sync_playwright() as p:
        # Launch browser có header giả lập
        browser = p.chromium.launch(headless=HEADLESS_MODE, args=['--start-maximized']) # Mở rộng cửa sổ
        
        # Tạo context với User Agent xịn (Giả làm Chrome trên Windows 10)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 720}
        )
        page = context.new_page()

        # --- TẠM TẮT CHẶN ẢNH ĐỂ ĐẢM BẢO LOAD ĐƯỢC DATA TRƯỚC ---
        # (Khi code chạy ngon rồi mới bật lại để tối ưu sau)
        # page.route("**/*", route_intercept) 
        # --------------------------------------------------------

        for city in cities:
            for check_in in date_list:
                for group in groups:
                    
                    full_url = generate_url(city, check_in, group)
                    print(f"--> {city} | {check_in.date()} | {group['id']}...", end=" ", flush=True)
                    
                    try:
                        page.goto(full_url, timeout=60000, wait_until="domcontentloaded")
                        
                        # Tăng thời gian chờ selector lên 10s (mạng chậm hoặc cần load JS)
                        try:
                            page.wait_for_selector('[data-testid="property-card"]', timeout=10000)
                        except:
                            # Nếu không thấy thẻ phòng, có thể do hiện CAPTCHA hoặc trang lỗi
                            print("❌ (Timeout/Captcha?)")
                            # Chụp ảnh lỗi để debug nếu cần
                            # page.screenshot(path=f"error_{city}_{check_in.date()}.png")
                            continue

                        cards = page.locator('[data-testid="property-card"]').all()
                        
                        count_local = 0
                        for card in cards:
                            if count_local >= LIMIT_ITEMS: break 

                            try:
                                link = card.locator('[data-testid="title-link"]').get_attribute("href")
                                address = card.locator('[data-testid="address"]').inner_text()
                                
                                price_locator = card.locator('[data-testid="price-and-discounted-price"]').first
                                if price_locator.count() > 0:
                                    price_raw = price_locator.inner_text()
                                    price_clean = float(price_raw.replace('VND', '').replace('.', '').replace(',', '').strip())
                                else:
                                    price_clean = 0
                                
                                all_results.append({
                                    'link': link.split('?')[0],
                                    'address': address,
                                    'city': city,
                                    'price': price_clean,
                                    'check_in_date': check_in.date(),
                                    'group_option': group['id'],
                                    'scanned_at': datetime.now().date()
                                })
                                count_local += 1
                            except:
                                continue 
                        
                        print(f"✅ Lấy {count_local} mục.")

                    except Exception as e:
                        print(f"❌ Error: {str(e)[:50]}...")
                    
                    # Nghỉ lâu hơn chút để giống người thật
                    time.sleep(2) 

        browser.close()

    print(f"🎉 Hoàn tất! Tổng cộng: {len(all_results)} bản ghi.")
    return pd.DataFrame(all_results)

# --- HÀM LƯU VÀO DB (Load Staging) ---
def run_crawler_and_load_staging():
    # 1. Crawl dữ liệu
    df = crawl_data()
    
    if df.empty:
        print("Không có dữ liệu nào được quét.")
        return

    # 2. Kết nối DB
    connection_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_string)

    # 3. Insert vào bảng booking_staging
    with engine.connect() as conn:
        # --- SỬA Ở ĐÂY ---
        # Bọc câu lệnh SQL trong hàm text()
        conn.execute(text("TRUNCATE TABLE booking_staging;"))
        conn.commit() # <--- QUAN TRỌNG: Phải commit thì lệnh mới có hiệu lực
        print("Đã làm sạch bảng booking_staging.")
        
    df.to_sql('booking_staging', engine, if_exists='append', index=False)
    print("✅ Đã đẩy dữ liệu vào bảng booking_staging thành công.")

if __name__ == "__main__":
    run_crawler_and_load_staging()