import pandas as pd
from pathlib import Path
from datetime import datetime

RAW = Path("weather_air_quality.csv")

# Thêm timestamp vào tên file (định dạng YYYYMMDD_HHMMSS)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_WEATHER = Path(f"weather_data_clean_{timestamp}.csv")
OUT_AIR     = Path(f"air_quality_data_clean_{timestamp}.csv")

# 1) Đọc CSV không có header chuẩn
df = pd.read_csv(RAW, header=None)

# 2) Gán header chuẩn
df.columns = [
    "datetime", "city",
    "temp", "humidity", "weather", "wind_speed",
    "aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10"
]

# 3) Loại dòng trống hoàn toàn
df = df.dropna(how="all")

# 4) Chuẩn hoá datetime & thay "N/A" → NaN
df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
df = df.replace("N/A", pd.NA)

# 5) Chuẩn hoá tên city
df["city"] = df["city"].astype(str).str.strip().str.title()

# 6) Bỏ trùng (theo city + datetime)
df = df.drop_duplicates(subset=["city", "datetime"])

# 7) Tách thành 2 DataFrame
weather_df = df[["datetime","city","temp","humidity","weather","wind_speed"]].copy()
air_df     = df[["datetime","city","aqi","co","no","no2","o3","so2","pm2_5","pm10"]].copy()

# 8) Xuất CSV sạch (UTF-8, không index)
weather_df.to_csv(OUT_WEATHER, index=False, encoding="utf-8")
air_df.to_csv(OUT_AIR, index=False, encoding="utf-8")

print(f"✅ Đã tạo file: {OUT_WEATHER}")
print(f"✅ Đã tạo file: {OUT_AIR}")
