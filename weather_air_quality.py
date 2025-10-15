import os
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ✅ Load biến môi trường
load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

VN_TZ_OFFSET = 7  # UTC+7

CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0678, "lon": 108.2208},
}

def to_vietnam_time(utc_ts: datetime):
    return utc_ts + timedelta(hours=VN_TZ_OFFSET)

# 🌤️ Lấy dữ liệu thời tiết
def get_weather(lat, lon):
    url = "https://api.openweathermap.org/data/2.5/weather"
    res = requests.get(url, params={
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }, timeout=30)
    res.raise_for_status()
    d = res.json()
    return {
        "temp": d["main"]["temp"],
        "humidity": d["main"]["humidity"],
        "weather": d["weather"][0]["main"],
        "wind_speed": d["wind"]["speed"],
    }

# 💨 Lấy dữ liệu chất lượng không khí
def get_air_quality(lat, lon):
    url = "https://api.openweathermap.org/data/2.5/air_pollution"
    res = requests.get(url, params={
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY
    }, timeout=30)
    res.raise_for_status()
    item = res.json()["list"][0]
    c = item["components"]
    return {
        "aqi": item["main"]["aqi"],
        "co": c["co"],
        "no": c["no"],
        "no2": c["no2"],
        "o3": c["o3"],
        "so2": c["so2"],
        "pm2_5": c["pm2_5"],
        "pm10": c["pm10"],
    }

# 🗄️ Ghi dữ liệu vào Supabase PostgreSQL
def save_to_db(city, weather, air):
    if not SUPABASE_DB_URL:
        raise RuntimeError("❌ SUPABASE_DB_URL is empty. Check GitHub Secrets!")

    conn = psycopg2.connect(SUPABASE_DB_URL)
    cur = conn.cursor()

    # ✅ Kiểm tra kết nối
    cur.execute("SELECT current_database(), current_user;")
    print("📡 Connected to:", cur.fetchone())

    # 🕒 Tạo timestamp giờ Việt Nam (có sẵn khác nhau mỗi giờ)
    ts_vn = to_vietnam_time(datetime.utcnow()).replace(microsecond=0)
    print(f"🕒 Timestamp VN: {ts_vn}")

    # 🏙️ Upsert thành phố
    cur.execute(
        """
        INSERT INTO Cities (city_name, latitude, longitude)
        VALUES (%s, %s, %s)
        ON CONFLICT (city_name) DO UPDATE
        SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
        RETURNING city_id;
        """,
        (city, CITIES[city]["lat"], CITIES[city]["lon"]),
    )
    city_id = cur.fetchone()[0]
    print(f"🏙️ City ID for {city}: {city_id}")

    # 🌤️ Ghi dữ liệu thời tiết
    cur.execute(
        """
        INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
        """,
        (city_id, ts_vn, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"]),
    )

    # 💨 Ghi dữ liệu chất lượng không khí
    cur.execute(
        """
        INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, ts) DO NOTHING;
        """,
        (city_id, ts_vn, air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"]),
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Đã lưu dữ liệu cho {city} lúc {ts_vn}\n")

# 🏁 Main
if __name__ == "__main__":
    for city, info in CITIES.items():
        try:
            print(f"🚀 Bắt đầu thu thập dữ liệu cho {city}")
            w = get_weather(info["lat"], info["lon"])
            a = get_air_quality(info["lat"], info["lon"])
            print("🌤️ Weather data:", w)
            print("💨 Air quality data:", a)
            save_to_db(city, w, a)
        except Exception as e:
            print(f"❌ Lỗi với {city}: {e}")
