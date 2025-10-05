import requests
import csv
import os
from datetime import datetime, timedelta

# 🔑 Lấy API Key từ GitHub Secrets
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("❌ OPENWEATHER_API_KEY chưa được thiết lập trong môi trường!")

# 🌍 Thành phố cần lấy dữ liệu
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022},
}

# 📂 File CSV duy nhất trong repo
CSV_FILE = "weather_air_quality.csv"

def get_weather(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return {
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["main"],
            "wind_speed": data["wind"]["speed"],
        }
    except Exception as e:
        print("⚠️ Weather API error:", e)
        return {"temp": "N/A", "humidity": "N/A", "weather": "N/A", "wind_speed": "N/A"}

def get_air_quality(lat, lon):
    try:
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()["list"][0]
        return {
            "aqi": data["main"]["aqi"],
            "co": data["components"]["co"],
            "no": data["components"]["no"],
            "no2": data["components"]["no2"],
            "o3": data["components"]["o3"],
            "so2": data["components"]["so2"],
            "pm2_5": data["components"]["pm2_5"],
            "pm10": data["components"]["pm10"],
        }
    except Exception as e:
        print("⚠️ Air API error:", e)
        return {"aqi": "N/A", "co": "N/A", "no": "N/A", "no2": "N/A", "o3": "N/A",
                "so2": "N/A", "pm2_5": "N/A", "pm10": "N/A"}

def crawl_and_save():
    # 🕒 Lấy timestamp (giờ Việt Nam)
    timestamp = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")

    # 🔍 Nếu file chưa có thì tạo và thêm header
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "datetime", "city",
                "temp", "humidity", "weather", "wind_speed",
                "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10"
            ])

    # ✏️ Append dữ liệu mới
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # 👉 Thêm 2 dòng trống để tách block dữ liệu giữa các lần crawl
        writer.writerow([])
        writer.writerow([])

        for city, coords in CITIES.items():
            weather = get_weather(coords["lat"], coords["lon"])
            air = get_air_quality(coords["lat"], coords["lon"])

            row = [
                timestamp, city,
                weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"],
                air["aqi"], air["co"], air["no"], air["no2"], air["o3"], air["so2"], air["pm2_5"], air["pm10"]
            ]
            writer.writerow(row)

    print(f"✅ Đã append dữ liệu mới vào {CSV_FILE} lúc {timestamp}")

if __name__ == "__main__":
    crawl_and_save()
