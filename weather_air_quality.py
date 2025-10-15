import os
import requests
from datetime import datetime, timedelta
import psycopg2

# 🌤️ API key từ GitHub Secrets
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# 🌐 URL Supabase từ GitHub Secrets
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

# 🏙️ Danh sách thành phố
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0678, "lon": 108.2208}
}

# 📌 Hàm trả về giờ Việt Nam (UTC+7)
def vn_now():
    return datetime.utcnow() + timedelta(hours=7)

# 🌤️ Lấy dữ liệu thời tiết từ OpenWeather
def get_weather(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"]
    }

# ☁️ Lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()["list"][0]
    comps = data["components"]
    return {
        "aqi": data["main"]["aqi"],
        "co": comps["co"],
        "no": comps["no"],
        "no2": comps["no2"],
        "o3": comps["o3"],
        "so2": comps["so2"],
        "pm2_5": comps["pm2_5"],
        "pm10": comps["pm10"]
    }

# 🗄️ Ghi dữ liệu vào PostgreSQL Supabase
def insert_data(city, weather, air):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Lấy city_id
    cur.execute("SELECT city_id FROM Cities WHERE city_name = %s;", (city,))
    result = cur.fetchone()
    if result:
        city_id = result[0]
    else:
        cur.execute("INSERT INTO Cities (city_name) VALUES (%s) RETURNING city_id;", (city,))
        city_id = cur.fetchone()[0]

    ts = vn_now().replace(second=0, microsecond=0)

    # 🌀 Insert vào WeatherData
    cur.execute("""
        INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (city_id, ts, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"]))

    # 🌫️ Insert vào AirQualityData
    cur.execute("""
        INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (city_id, ts, air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Đã ghi dữ liệu cho {city} vào DB lúc {ts}")

# 🚀 Chạy chính
if __name__ == "__main__":
    for city, info in CITIES.items():
        print(f"🚀 Bắt đầu thu thập dữ liệu cho {city}")
        try:
            weather = get_weather(info["lat"], info["lon"])
            print("🌤️ Weather data:", weather)

            air = get_air_quality(info["lat"], info["lon"])
            print("🌫️ Air quality data:", air)

            insert_data(city, weather, air)
        except Exception as e:
            print(f"❌ Lỗi với {city}:", e)
