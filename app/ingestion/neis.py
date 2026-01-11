from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List

from ..config import get_settings
from ..schemas.data import MealDocument, MealMenuItem, ScheduleDocument, ScheduleEntry, TimetableDocument
from .http_client import get_json

_settings = get_settings()


def _load_delicious_keywords() -> List[str]:
    data_path = Path(__file__).resolve().parent.parent / "data" / "delicious.txt"
    if not data_path.exists():
        return []
    return [line.strip() for line in data_path.read_text(encoding="utf-8").splitlines() if line.strip()]


_DELICIOUS_KEYWORDS = _load_delicious_keywords()

_ALLERGY_PATTERN = re.compile(r"([0-9]+)\.")


async def _get_json(url: str, params: Dict[str, str], label: str) -> Dict:
    return await get_json(url, params=params, timeout=10.0, retries=2, backoff=0.5, label=label)


async def fetch_meals(start: date, end: date) -> Dict[str, MealDocument]:
    url = "https://open.neis.go.kr/hub/mealServiceDietInfo"
    params = {
        "KEY": _settings.neis_api_key,
        "Type": "json",
        "ATPT_OFCDC_SC_CODE": _settings.neis_atpt_code,
        "SD_SCHUL_CODE": _settings.neis_school_code,
        "MMEAL_SC_CODE": "2",
        "MLSV_FROM_YMD": start.strftime("%Y%m%d"),
        "MLSV_TO_YMD": end.strftime("%Y%m%d"),
    }

    data = await _get_json(url, params, "neis.meals")

    payload: Dict[str, MealDocument] = {}
    service_data = data.get("mealServiceDietInfo")
    if not service_data or len(service_data) < 2:
        return payload

    rows = service_data[1].get("row", [])
    for item in rows:
        day = datetime.strptime(item["MLSV_YMD"], "%Y%m%d").date()
        menus_raw = item.get("DDISH_NM", "").replace("<br/>", "\n").split("\n")
        menus: List[MealMenuItem] = []
        menus_plain: List[str] = []
        for menu in menus_raw:
            allergies = [
                int(m.group(1))
                for m in _ALLERGY_PATTERN.finditer(menu)
                if 1 <= int(m.group(1)) <= 18
            ]
            cleaned = _ALLERGY_PATTERN.sub("", menu).replace("()", "").strip()
            cleaned = re.sub(r"[ #&*-.=@_]+$", "", cleaned)
            if cleaned:
                if any(keyword in cleaned for keyword in _DELICIOUS_KEYWORDS):
                    cleaned = f"⭐{cleaned}"
                menus.append(MealMenuItem(name=cleaned, allergies=allergies))
                menus_plain.append(cleaned)
        calories = item.get("CAL_INFO")
        try:
            calories_value = float(calories.replace(" Kcal", "")) if calories else None
        except ValueError:
            calories_value = None

        payload[_date_key(day)] = MealDocument(
            _id=_date_key(day),
            date=day,
            menus=menus,
            menus_plain=menus_plain,
            calories=calories_value,
        )
    return payload


async def fetch_schedule(start: date, end: date) -> Dict[str, ScheduleDocument]:
    url = "https://open.neis.go.kr/hub/SchoolSchedule"
    params = {
        "KEY": _settings.neis_api_key,
        "Type": "json",
        "ATPT_OFCDC_SC_CODE": _settings.neis_atpt_code,
        "SD_SCHUL_CODE": _settings.neis_school_code,
        "AA_FROM_YMD": start.strftime("%Y%m%d"),
        "AA_TO_YMD": end.strftime("%Y%m%d"),
    }

    data = await _get_json(url, params, "neis.schedule")

    payload: Dict[str, ScheduleDocument] = {}
    root = data.get("SchoolSchedule")
    if not root or len(root) < 2:
        return payload

    rows = root[1].get("row", [])
    grouped: Dict[str, List[ScheduleEntry]] = defaultdict(list)

    for row in rows:
        if row.get("EVENT_NM") == "토요휴업일":
            continue
        day = datetime.strptime(row["AA_YMD"], "%Y%m%d").date()
        grades = []
        if row.get("ONE_GRADE_EVENT_YN") == "Y":
            grades.append(1)
        if row.get("TW_GRADE_EVENT_YN") == "Y":
            grades.append(2)
        if row.get("THREE_GRADE_EVENT_YN") == "Y":
            grades.append(3)
        if row.get("FR_GRADE_EVENT_YN") == "Y":
            grades.append(4)
        if row.get("FIV_GRADE_EVENT_YN") == "Y":
            grades.append(5)
        if row.get("SIX_GRADE_EVENT_YN") == "Y":
            grades.append(6)
        grouped[_date_key(day)].append(ScheduleEntry(name=row["EVENT_NM"].strip(), grades=grades))

    for key, entries in grouped.items():
        summary_lines = []
        for entry in entries:
            suffix = "({})".format(", ".join(f"{grade}학년" for grade in entry.grades)) if entry.grades else ""
            summary_lines.append(f"{entry.name}{suffix}")
        summary = "\n".join(summary_lines).replace("()", "")
        payload[key] = ScheduleDocument(
            _id=key,
            date=datetime.fromisoformat(key).date(),
            entries=entries if entries else None,
            summary=summary or None,
        )

    return payload


async def fetch_timetable(start: date, end: date) -> Dict[str, TimetableDocument]:
    url = "https://open.neis.go.kr/hub/hisTimetable"
    params = {
        "KEY": _settings.neis_api_key,
        "Type": "json",
        "pSize": "1000",
        "ATPT_OFCDC_SC_CODE": _settings.neis_atpt_code,
        "SD_SCHUL_CODE": _settings.neis_school_code,
        "TI_FROM_YMD": start.strftime("%Y%m%d"),
        "TI_TO_YMD": end.strftime("%Y%m%d"),
    }

    lessons: Dict[str, Dict[str, Dict[str, List[str]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    page = 1
    while True:
        page_params = params | {"pIndex": str(page)}
        data = await _get_json(url, page_params, "neis.timetable")
        root = data.get("hisTimetable")
        if not root or len(root) < 2:
            break
        rows = root[1].get("row", [])
        if not rows:
            break
        for row in rows:
            class_name = row.get("CLASS_NM")
            subject = row.get("ITRT_CNTNT")
            grade_value = row.get("GRADE")
            if not class_name or not subject or subject == "토요휴업일" or not grade_value:
                continue
            try:
                class_key = str(int(class_name))
                grade_key = str(int(grade_value))
            except (TypeError, ValueError):
                continue
            key = row.get("ALL_TI_YMD")
            day = datetime.strptime(key, "%Y%m%d").date()
            lessons[_date_key(day)][grade_key][class_key].append(subject)
        if len(rows) < 1000:
            break
        page += 1

    payload: Dict[str, TimetableDocument] = {}
    for key, grade_map in lessons.items():
        normalized = {
            grade: {clazz: slots for clazz, slots in sorted(classes.items(), key=lambda item: int(item[0]))}
            for grade, classes in grade_map.items()
        }
        payload[key] = TimetableDocument(
            _id=key,
            date=datetime.fromisoformat(key).date(),
            lessons=normalized,
        )
    return payload


def _date_key(target: date) -> str:
    return target.isoformat()


async def fetch_all(start: date, end: date) -> Dict[str, Dict[str, object]]:
    meal_data, schedule_data, timetable_data = await asyncio.gather(
        fetch_meals(start, end),
        fetch_schedule(start, end),
        fetch_timetable(start, end),
    )
    return {
        "meal": meal_data,
        "schedule": schedule_data,
        "timetable": timetable_data,
    }
