from __future__ import annotations

import datetime
import xml.etree.ElementTree as ET
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

from ..config import get_settings
from ..schemas.data import WaterTemperatureDocument, WeatherDocument

_settings = get_settings()
_KST = ZoneInfo("Asia/Seoul")


async def fetch_weather() -> Optional[WeatherDocument]:
    url = "https://www.kma.go.kr/wid/queryDFSRSS.jsp"
    params = {"zone": _settings.kma_zone}

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        return None
    data_nodes = root.findall(".//data")
    if not data_nodes:
        return None

    weather = None
    for node in data_nodes[:6]:
        hour = node.findtext("hour")
        if hour == "9":
            weather = node
            break
    if weather is None:
        weather = data_nodes[0]

    def _safe_text(node_name: str, default: str = "") -> str:
        value = weather.findtext(node_name)
        return value if value is not None else default

    def _map_sky(value: str) -> str:
        mapping = {
            "1": "☀ 맑음",
            "2": "🌤️ 구름 조금",
            "3": "🌥️ 구름 많음",
            "4": "☁ 흐림",
        }
        return mapping.get(value, "⚠ 오류")

    def _map_pty(value: str) -> str:
        mapping = {
            "0": "❌ 없음",
            "1": "🌧️ 비",
            "2": "🌨️ 비/눈",
            "3": "🌨️ 눈",
        }
        return mapping.get(value, "⚠ 오류")

    now = datetime.datetime.now(_KST)
    timestamp_kst = datetime.datetime(
        now.year,
        now.month,
        now.day,
        int(weather.findtext("hour", "0")),
        tzinfo=_KST,
    )
    timestamp = timestamp_kst.astimezone(datetime.timezone.utc)
    first_hour = data_nodes[0].findtext("hour", "0")
    if first_hour == "24":
        first_hour = "0"

    return WeatherDocument(
        timestamp=timestamp,
        temp=_safe_text("temp"),
        temp_min=_safe_text("tmn"),
        temp_max=_safe_text("tmx"),
        sky=_map_sky(_safe_text("sky")),
        pty=_map_pty(_safe_text("pty")),
        precip_probability=_safe_text("pop"),
        humidity=_safe_text("reh"),
        first_hour=first_hour,
    )


async def fetch_water_temperature() -> Optional[WaterTemperatureDocument]:
    url = f"http://openapi.seoul.go.kr:8088/{_settings.seoul_data_token}/json/WPOSInformationTime/1/5/"

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url)
        response.raise_for_status()

    data = response.json()
    try:
        rows = data["WPOSInformationTime"]["row"]
    except (KeyError, TypeError):
        return None

    try:
        measurement = rows[0]
        timestamp_local = datetime.datetime.strptime(
            f"{measurement['MSR_DATE']} {measurement['MSR_TIME']}", "%Y%m%d %H:%M"
        )
    except (IndexError, KeyError, ValueError):
        return None
    timestamp = timestamp_local.replace(tzinfo=_KST).astimezone(datetime.timezone.utc)

    temperatures = []
    for row in rows:
        value = row.get("W_TEMP")
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
