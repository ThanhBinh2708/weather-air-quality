import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔑 Tải biến môi trường từ GitHub Secrets (hoặc từ .env khi chạy local)
load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # ✅ tên phải trùng với Secrets

# 🕒 Thiết lập múi giờ Việt Nam
VN_TZ_OFFSET = 7  # UTC+7

# 🌍 Danh sách thành phố cần crawl
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0678, "lon": 108.2208}
}

# 🕐 Hàm chuyển UTC → giờ Việt Nam
def to_vietnam_time(utc_ts: datetime):
    return utc_ts + timedelta(hours=VN_TZ_OFFSET)

# 🌤️ Hàm lấy dữ liệu thời tiết
def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"]
    }

# 🌫️ Hàm lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    res = requests.get(url)
    res.raise_for_status()
    item = res.json()["list"][0]
    return {
        "aqi": item["main"]["aqi"],
        "co": item["components"]["co"],
        "no": item["components"]["no"],
        "no2": item["components"]["no2"],
        "o3": item["components"]["o3"],
        "so2": item["components"]["so2"],
        "pm2_5": item["components"]["pm2_5"],
        "pm10": item["components"]["pm10"],
    }

# 📊 Ghi dữ liệu trực tiếp vào Supabase
def save_to_db(city, weather, air):
    # ✅ Kết nối tới database Supabase
    conn = psycopg2.connect(SUPABASE_DB_URL)
    cur = conn.cursor()

    # ⏱️ Timestamp theo giờ Việt Nam
    ts_vn = to_vietnam_time(datetime.utcnow())

    # 🏙️ Kiểm tra / tạo city_id
    cur.execute("SELECT city_id FROM Cities WHERE city_name = %s;", (city,))
    result = cur.fetchone()
    if result:
        city_id = result[0]
    else:
        cur.execute("INSERT INTO Cities (city_name) VALUES (%s) RETURNING city_id;", (city,))
        city_id = cur.fetchone()[0]

    # ☁️ Ghi dữ liệu thời tiết
    cur.execute("""
        INSERT INTO WeatherData
