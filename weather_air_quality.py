import requests
import csv
import os
from datetime import datetime

# Lấy API Key từ biến môi trường (set trong GitHub Secrets)
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("❌ OPENWEATHER_API_KEY chưa được thiết lập trong môi trường!")

# Thành phố và tọa độ
CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022},
}

# Tên file CSV
CSV_FILE = "weather_air_quality.csv"

def get_weather(lat, lon):
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

def get_air_quality(lat, lon):
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

def crawl_and_save():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Nếu file chưa tồn tại thì ghi header
    try:
        with open(CSV_FILE, "x", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "city", "temp", "humidity", "weather", "wind_speed",
                "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10"
            ])
    except FileExistsError:
        pass

    # Ghi dữ liệu cho 2 thành phố
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for city, coords in CITIES.items():
            weather = get_weather(coords["lat"], coords["lon"])
            air = get_air_quality(coords["lat"], coords["lon"])
            row = [
                timestamp, city,
                weather["temp"], weather["humidity"], weather["weather"], weather["wind_speed"],
                air["aqi"], air["co"], air["no"], air["no2"], air["o3"],
                air["so2"], air["pm2_5"], air["pm10"]
            ]
            writer.writerow(row)

        # Chèn 1 dòng trống sau mỗi lần crawl
        writer.writerow([])

if __name__ == "__main__":
    crawl_and_save()
    print("✅ Dữ liệu đã được lưu vào", CSV_FILE)
