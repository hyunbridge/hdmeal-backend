from __future__ import annotations

import datetime

from typing import Optional
from zoneinfo import ZoneInfo

from ..config import get_settings
from ..schemas.data import WaterTemperatureDocument, WeatherDocument
from .http_client import get_json

_settings = get_settings()
_KST = ZoneInfo("Asia/Seoul")


async def fetch_weather() -> Optional[WeatherDocument]:
    api_key = _settings.kma_api_key
    nx = _settings.kma_nx
    ny = _settings.kma_ny

    # Determine base_date and base_time
    # API updates at 02:10, 05:10, 08:10, 11:10, 14:10, 17:10, 20:10, 23:10
    now = datetime.datetime.now(_KST)
    
    # Adjust to find the latest available base_time
    # If we are before 02:10, we must use yesterday's 23:00
    if now.hour < 2 or (now.hour == 2 and now.minute < 10):
        base_dt = now - datetime.timedelta(days=1)
        base_date = base_dt.strftime("%Y%m%d")
        base_time = "2300"
    else:
        # Possible base hours: 2, 5, 8, 11, 14, 17, 20, 23
        # We need the largest one such that (hour, 10) <= current_time
        base_date = now.strftime("%Y%m%d")
        
        # Check from 23 down to 2
        found_hour = 2
        for h in [23, 20, 17, 14, 11, 8, 5, 2]:
            # API is available ~10 mins after the hour
            check_time = now.replace(hour=h, minute=10, second=0, microsecond=0)
            if now >= check_time:
                found_hour = h
                break
        
        base_time = f"{found_hour:02d}00"

    url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    params = {
        "serviceKey": api_key,
        "pageNo": "1",
        "numOfRows": "1000", # Fetch enough to cover next 24-48h
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny),
    }

    try:
        data = await get_json(url, params=params, timeout=10.0, retries=2, backoff=0.5, label="kma.weather")
    except Exception:
        return None

    try:
        result_code = data.get("response", {}).get("header", {}).get("resultCode")
        if result_code and result_code != "00":
            return None
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return None

    if not items:
        return None

    # Identify 'representative' time slot. (0900 of today, or nearest if passed)
    today_str = now.strftime("%Y%m%d")
    tomorrow_str = (now + datetime.timedelta(days=1)).strftime("%Y%m%d")
    
    representative_item = None
    
    # First, try to find 0900 for today (if in future relative to base_time)
    for item in items:
        if item["category"] == "TMP" and item["fcstDate"] == today_str and item["fcstTime"] == "0900":
            representative_item = item
            break
            
    # If not found (e.g. passed), and now > 17:00, maybe look for Tomorrow 0900?
    if not representative_item and now.hour >= 17:
        for item in items:
             if item["category"] == "TMP" and item["fcstDate"] == tomorrow_str and item["fcstTime"] == "0900":
                representative_item = item
                break
                
    # If still not found, just take the first TMP item (nearest)
    if not representative_item:
        for item in items:
            if item["category"] == "TMP":
                representative_item = item
                break
    
    if not representative_item:
        return None
        
    rep_date = representative_item["fcstDate"]
    rep_time = representative_item["fcstTime"]
    
    # Now extract all params for this specific time slot
    def get_val_for_slot(cat):
        for item in items:
            if item["fcstDate"] == rep_date and item["fcstTime"] == rep_time and item["category"] == cat:
                return item["fcstValue"]
        return ""

    temp = get_val_for_slot("TMP")
    sky_code = get_val_for_slot("SKY")
    pty_code = get_val_for_slot("PTY")
    pop = get_val_for_slot("POP")
    reh = get_val_for_slot("REH")
    
    # Get TMX and TMN for the *representative date*
    # TMN is usually at 0600, TMX at 1500.
    # We scan ALL items for this date.
    tmn = ""
    tmx = ""
    for item in items:
        if item["fcstDate"] == rep_date:
            if item["category"] == "TMN":
                tmn = item["fcstValue"]
            elif item["category"] == "TMX":
                tmx = item["fcstValue"]

    # Mapping
    def _map_sky(val):
        # 1: Clear, 3: Many Clouds, 4: Cloudy
        m = {"1": "☀ 맑음", "3": "🌥️ 구름 많음", "4": "☁ 흐림"}
        return m.get(val, "Unknown")
        
    def _map_pty(val):
        # 0: None, 1: Rain, 2: Rain/Snow, 3: Snow, 4: Shower
        m = {"0": "❌ 없음", "1": "🌧️ 비", "2": "🌨️ 비/눈", "3": "🌨️ 눈", "4": "🚿 소나기"}
        return m.get(val, "⚠ 오류")

    timestamp = datetime.datetime.strptime(f"{rep_date} {rep_time}", "%Y%m%d %H%M").replace(tzinfo=_KST).astimezone(datetime.timezone.utc)
    
    first_hour = str(int(rep_time[:2]))

    return WeatherDocument(
        timestamp=timestamp,
        temp=temp,
        temp_min=tmn if tmn else "-", 
        temp_max=tmx if tmx else "-",
        sky=_map_sky(sky_code),
        pty=_map_pty(pty_code),
        precip_probability=pop,
        humidity=reh,
        first_hour=first_hour,
    )


async def fetch_water_temperature() -> Optional[WaterTemperatureDocument]:
    url = f"http://openapi.seoul.go.kr:8088/{_settings.seoul_data_token}/json/WPOSInformationTime/1/5/"

    data = await get_json(url, timeout=5.0, retries=2, backoff=0.5, label="seoul.water")
    try:
        rows = data["WPOSInformationTime"]["row"]
    except (KeyError, TypeError):
        return None

    try:
        measurement = rows[0]
        timestamp_local = datetime.datetime.strptime(
            f"{measurement['YMD']} {measurement['HR']}", "%Y%m%d %H:%M"
        )
    except (IndexError, KeyError, ValueError):
        return None
    timestamp = timestamp_local.replace(tzinfo=_KST).astimezone(datetime.timezone.utc)

    temperatures = []
    for row in rows:
        value = row.get("WATT")
        try:
            temperatures.append(float(value))
        except (TypeError, ValueError):
            continue

    if not temperatures:
        return None

    avg_temp = sum(temperatures) / len(temperatures)

    return WaterTemperatureDocument(
        timestamp=timestamp,
        temperature_c=round(avg_temp, 2),
    )
