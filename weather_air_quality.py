import os
import requests
import psycopg2
from datetime import datetime
import pytz

# 📦 Đọc biến môi trường
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# 🏙️ Danh sách thành phố
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022}
}

# 🌏 Hàm lấy giờ Việt Nam
def vn_time():
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    return datetime.now(tz).replace(second=0, microsecond=0)

# ☁️ Lấy dữ liệu thời tiết
def get_weather(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    r = requests.get(url)
    data = r.json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"]
    }

# 🌫️ Lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    r = requests.get(url)
    data = r.json()["list"][0]
    return {
        "aqi": data["main"]["aqi"],
        "co": data["components"]["co"],
        "no": data["components"]["no"],
        "no2": data["components"]["no2"],
        "o3": data["components"]["o3"],
        "so2": data["components"]["so2"],
        "pm2_5": data["components"]["pm2_5"],
        "pm10": data["components"]["pm10"]
    }

# 📤 Ghi dữ liệu vào Supabase
def insert_data(city, weather, air):
    conn = psycopg2.connect(SUPABASE_DB_URL)  # ✅ KHÔNG có sslmode
    cur = conn.cursor()

    ts = vn_time()

    # Insert vào WeatherData
    cur.execute("""
        INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
        SELECT city_id, %s, %s, %s, %s, %s FROM Cities WHERE city_name = %s
        ON CONFLICT (city_id, ts) DO NOTHING
    """, (ts, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"], city))

    # Insert vào AirQualityData
    cur.execute("""
        INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
        SELECT city_id, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM Cities WHERE city_name = %s
        ON CONFLICT (city_id, ts) DO NOTHING
    """, (
        ts, air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"], city
    ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Đã insert dữ liệu cho {city} lúc {ts}")

# 🏁 Main
if __name__ == "__main__":
    for city, coords in CITIES.items():
        print(f"🚀 Bắt đầu thu thập dữ liệu cho {city}")
        weather = get_weather(coords["lat"], coords["lon"])
        air = get_air_quality(coords["lat"], coords["lon"])
        print("🌤 Weather:", weather)
        print("🌫 Air:", air)
        insert_data(city, weather, air)
