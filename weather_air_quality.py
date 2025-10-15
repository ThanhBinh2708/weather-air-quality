import os
import psycopg2
import requests
from datetime import datetime, timedelta

# 🌤 API Keys
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# Thành phố
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0678, "lon": 108.2208},
}

# Hàm lấy dữ liệu thời tiết
def get_weather(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    data = requests.get(url).json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"],
    }

# Hàm lấy dữ liệu chất lượng không khí
def get_air(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    data = requests.get(url).json()
    c = data["list"][0]["components"]
    return {
        "aqi": data["list"][0]["main"]["aqi"],
        "co": c["co"],
        "no": c["no"],
        "no2": c["no2"],
        "o3": c["o3"],
        "so2": c["so2"],
        "pm2_5": c["pm2_5"],
        "pm10": c["pm10"],
    }

# 🛢 Ghi thẳng vào bảng chính
def insert_data(city, weather, air):
    conn = psycopg2.connect(SUPABASE_DB_URL, sslmode="require")
    cur = conn.cursor()

    ts = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")

    # Lấy city_id từ bảng Cities
    cur.execute("SELECT city_id FROM Cities WHERE city_name=%s", (city,))
    row = cur.fetchone()
    if row:
        city_id = row[0]
    else:
        cur.execute(
            "INSERT INTO Cities (city_name, latitude, longitude) VALUES (%s,%s,%s) RETURNING city_id",
            (city, CITIES[city]["lat"], CITIES[city]["lon"]),
        )
        city_id = cur.fetchone()[0]

    # Thời tiết
    cur.execute("""
        INSERT INTO WeatherData (city_id, ts, temp, humidity, weather, wind_speed)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (city_id, ts, weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"]))

    # Không khí
    cur.execute("""
        INSERT INTO AirQualityData (city_id, ts, aqi, co, no, no2, o3, so2, pm2_5, pm10)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (city_id, ts) DO NOTHING;
    """, (city_id, ts, air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Insert thành công dữ liệu cho {city} lúc {ts}")

# 🚀 Chạy toàn bộ pipeline
if __name__ == "__main__":
    for city, coords in CITIES.items():
        print(f"📡 Bắt đầu thu thập dữ liệu cho {city}")
        weather = get_weather(coords["lat"], coords["lon"])
        air = get_air(coords["lat"], coords["lon"])
        insert_data(city, weather, air)
