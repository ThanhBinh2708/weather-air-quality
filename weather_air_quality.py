import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔑 Tải biến môi trường từ Secrets (.env nếu chạy local)
load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
SUPABASE_DB_URL = os.getenv("DB_URL")  # Dạng: postgresql://user:pass@host:port/db

# 🕒 Thiết lập múi giờ Việt Nam
VN_TZ_OFFSET = 7  # UTC+7

# 🌍 Danh sách thành phố
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0678, "lon": 108.2208}
}

# 📦 Hàm chuyển giờ UTC → giờ Việt Nam
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
    data = res.json()["list"][0]["components"]
    aqi = res.json()["list"][0]["main"]["aqi"]
    return {
        "aqi": aqi,
        "co": data["co"],
        "no": data["no"],
        "no2": data["no2"],
        "o3": data["o3"],
        "so2": data["so2"],
        "pm2_5": data["pm2_5"],
        "pm10": data["pm10"]
    }

# 📊 Ghi dữ liệu trực tiếp vào Supabase
def save_to_db(city, weather, air):
    conn = psycopg2.connect(SUPABASE_DB_URL)
    cur = conn.cursor()

    # ⏱️ Timestamp giờ Việt Nam
    ts_vn = to_vietnam_time(datetime.utcnow())

    # 🏙️ Lấy city_id từ bảng Cities
    cur.execute("SELECT city_id FROM Cities WHERE city_name = %s;", (city,))
    result = cur.fetchone()
    if result:
        city_id = result[0]
    else:
        cur.execute("INSERT INTO Cities (city_name) VALUES (%s) RETURNING city_id;", (city,))
        city_id = cur.fetchone()[0]

    # ☁️ Ghi vào WeatherData
    cur.execute("""
        INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (city_id, ts_vn, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"]))

    # 💨 Ghi vào AirQualityData
    cur.execute("""
        INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (
        city_id, ts_vn, air["aqi"], air["co"], air["no"], air["no2"],
        air["o3"], air["so2"], air["pm2_5"], air["pm10"]
    ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Dữ liệu đã lưu vào DB cho {city} lúc {ts_vn}")

# 🚀 Main
if __name__ == "__main__":
    for city, info in CITIES.items():
        try:
            weather = get_weather(info["lat"], info["lon"])
            air = get_air_quality(info["lat"], info["lon"])
            save_to_db(city, weather, air)
        except Exception as e:
            print(f"❌ Lỗi với {city}: {e}")
