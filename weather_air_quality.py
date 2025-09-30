import requests
import csv
import os
from datetime import datetime

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("❌ OPENWEATHER_API_KEY chưa được thiết lập trong môi trường!")

CITIES = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Danang": {"lat": 16.0544, "lon": 108.2022},
}

CSV_FILE = "weather_air_quality.csv"

def get_weather(lat, lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    return {
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "aqi": data["main"].get("aqi", "N/A"),  # Một số gói API miễn phí không có AQI
        "weather": data["weather"][0]["description"],
    }

def write_csv():
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Nếu file chưa tồn tại, viết header
        if not file_exists:
            writer.writerow(["datetime", "city", "temp", "humidity", "aqi", "weather"])

        # Ghi dữ liệu cho từng thành phố
        for city, coords in CITIES.items():
            weather = get_weather(coords["lat"], coords["lon"])
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                city,
                weather["temp"],
                weather["humidity"],
                weather["aqi"],
                weather["weather"]
            ])

if __name__ == "__main__":
    write_csv()
