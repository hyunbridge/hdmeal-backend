from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

from ..config import get_settings
from ..schemas.data import MealDocument, ScheduleDocument, TimetableDocument
from ..utils import security
from .data_service import DataService
from .ingestion_service import IngestionService
from .user_service import UserService

_settings = get_settings()
_ALLERGY_LABELS = [
    "",
    "난류",
    "우유",
    "메밀",
    "땅콩",
    "대두",
    "밀",
    "고등어",
    "게",
    "새우",
    "돼지고기",
    "복숭아",
    "토마토",
    "아황산류",
    "호두",
    "닭고기",
    "쇠고기",
    "오징어",
    "조개류",
]

_KST = ZoneInfo("Asia/Seoul")
LegacyResponse = Tuple[List[Any], Optional[Any], Optional[str]]


class ChatbotService:
    def __init__(self, data_service: DataService, ingestion_service: IngestionService):
        self._data_service = data_service
        self._ingestion_service = ingestion_service
        self._user_service = UserService(data_service)

    async def handle_intent(
        self,
        platform: str,
        external_id: str,
        intent: str,
        params: Dict[str, Any],
        req_id: str,
    ) -> LegacyResponse:
        try:
            if "Briefing" in intent:
                return await self._briefing(platform, external_id, req_id)
            if "Meal" in intent:
                return await self._meal(platform, external_id, params, req_id)
            if "Timetable" in intent:
                return await self._timetable(platform, external_id, params, req_id)
            if "Schedule" in intent:
                return await self._schedule(params, req_id)
            if "WaterTemperature" in intent:
                return await self._water_temperature(req_id)
            if "UserSettings" in intent:
                return self._user_settings_card(platform, external_id, req_id)
            if "ModifyUserInfo" in intent:
                return await self._modify_user_info(platform, external_id, params, req_id)
            return (["잘못된 요청입니다.\n요청 ID: " + req_id], None, None)
        except OSError:
            return (["알 수 없는 오류가 발생했습니다.\n요청 ID: " + req_id], None, None)

    async def _meal(
        self, platform: str, external_id: str, params: Dict[str, Any], req_id: str
    ) -> LegacyResponse:
        try:
            if not params.get("date"):
                return (["언제의 급식을 조회하시겠어요?"], None, None)
            value = params["date"]
            if isinstance(value, datetime):
                target_dt = value
            else:
                return (
                    [
                        "정확한 날짜를 입력해주세요.\n현재 식단조회에서는 여러날짜 조회를 지원하지 않습니다."
                    ],
                    None,
                    None,
                )
            if target_dt.weekday() >= 5:
                return (["급식을 실시하지 않습니다. (주말)"], None, None)

            meal_data = await self._build_meal_data(target_dt.date(), req_id)
            if "message" not in meal_data:
                grade, class_no, preferences = await self._legacy_get_user_info(
                    platform, external_id
                )
                pref = preferences.get("AllergyInfo", "Number")
                menus: List[str] = []
                for name, allergies in meal_data["menu"]:
                    if pref == "None" or not allergies:
                        menus.append(name)
                    elif pref == "FullText":
                        labels = [
                            _ALLERGY_LABELS[idx]
                            for idx in allergies
                            if 0 <= idx < len(_ALLERGY_LABELS)
                        ]
                        if labels:
                            menus.append(f"{name}({', '.join(labels)})")
                        else:
                            menus.append(name)
                    else:
                        labels = [str(idx) for idx in allergies]
                        if labels:
                            menus.append(f"{name}({', '.join(labels)})")
                        else:
                            menus.append(name)
                message = (
                    f"{meal_data['date']}:\n" + "\n".join(menus)
                    + f"\n\n열량: {meal_data['kcal']} kcal"
                )
                return ([message], None, None)

            if meal_data["message"] == "등록된 데이터가 없습니다.":
                schedule_text = await self._fetch_schedule_text(target_dt.date(), req_id)
                if schedule_text != "일정이 없습니다.":
                    return (["급식을 실시하지 않습니다. (%s)" % schedule_text], None, None)
            return ([meal_data["message"]], None, None)
        except ConnectionError:
            return (["급식 서버에 연결하지 못했습니다.\n요청 ID: " + req_id], None, None)

    async def _timetable(
        self, platform: str, external_id: str, params: Dict[str, Any], req_id: str
    ) -> LegacyResponse:
        suggest_to_register = False
        try:
            if (
                params.get("grade")
                and params.get("class")
                and str(params["grade"]).strip()
                and str(params["class"]).strip()
            ):
                try:
                    tt_grade = int(params["grade"])
                    tt_class = int(params["class"])
                except (TypeError, ValueError):
                    return (["올바른 숫자를 입력해 주세요."], None, None)
                if platform == "KT":
                    suggest_to_register = True
            else:
                grade, class_no, _ = await self._legacy_get_user_info(platform, external_id)
                if not grade or not class_no:
                    if platform == "KT":
                        return (
                            [
                                {
                                    "type": "card",
                                    "title": "사용자 정보를 찾을 수 없습니다.",
                                    "body": '"내 정보 관리"를 눌러 학년/반 정보를 등록 하시거나, '
                                    '"1학년 1반 시간표 알려줘"와 같이 조회할 학년/반을 직접 언급해 주세요.',
                                    "buttons": [{"type": "message", "title": "내 정보 관리"}],
                                }
                            ],
                            None,
                            None,
                        )
                    return (
                        [
                            '사용자 정보를 찾을 수 없습니다. "내 정보 관리"를 눌러 학년/반 정보를 등록해 주세요.'
                        ],
                        None,
                        None,
                    )
                tt_grade = grade
                tt_class = class_no

            if not params.get("date"):
                return (["언제의 시간표를 조회하시겠어요?"], None, None)
            if not isinstance(params["date"], datetime):
                return (
                    [
                        "정확한 날짜를 입력해주세요.\n현재 시간표조회에서는 여러날짜 조회를 지원하지 않습니다."
                    ],
                    None,
                    None,
                )
            target_dt = params["date"]
            timetable_text = await self._build_timetable_text(
                tt_grade, tt_class, target_dt.date(), req_id
            )
            if suggest_to_register:
                return (
                    [
                        timetable_text,
                        {
                            "type": "card",
                            "title": "방금 입력하신 정보를 저장할까요?",
                            "body": "학년/반 정보를 등록하시면 다음부터 더 빠르고 편하게 이용하실 수 있습니다.",
                            "buttons": [
                                {
                                    "type": "message",
                                    "title": "네, 저장해 주세요.",
                                    "postback": "사용자 정보 등록: %d학년 %d반"
                                    % (tt_grade, tt_class),
                                }
                            ],
                        },
                    ],
                    None,
                    None,
                )
            return ([timetable_text], None, None)
        except ConnectionError:
            return (["시간표 서버에 연결하지 못했습니다.\n요청 ID: " + req_id], None, None)

    async def _schedule(self, params: Dict[str, Any], req_id: str) -> LegacyResponse:
        try:
            if "date" not in params or not params["date"]:
                return (["언제의 학사일정을 조회하시겠어요?"], None, None)
            value = params["date"]
            if isinstance(value, datetime):
                schedule_text = await self._fetch_schedule_text(value.date(), req_id)
                if schedule_text and schedule_text != "일정이 없습니다.":
                    message = (
                        f"{value.year:04d}-{value.month:02d}-{value.day:02d}({self._weekday_ko(value)})"
                        f":\n{schedule_text}"
                    )
                else:
                    message = "일정이 없습니다."
                return ([message], None, None)
            if isinstance(value, list):
                try:
                    start = value[0]
                    end = value[1]
                except Exception:
                    return (["오류가 발생했습니다.\n요청 ID: " + req_id], None, None)
                if not isinstance(start, datetime) or not isinstance(end, datetime):
                    return (["오류가 발생했습니다.\n요청 ID: " + req_id], None, None)
                head, body = await self._build_schedule_range_message(start, end, req_id)
                return ([head + body], None, None)
            return (["언제의 학사일정을 조회하시겠어요?"], None, None)
        except ConnectionError:
            return (["학사일정 서버에 연결하지 못했습니다.\n요청 ID: " + req_id], None, None)

    async def _briefing(self, platform: str, external_id: str, req_id: str) -> LegacyResponse:
        """Generate a comprehensive briefing for the user."""
        target, date_label = self._get_briefing_target_date()

        if target.weekday() >= 5:
            return ([f"{date_label}은 주말 입니다."], None, None)

        header = f"{date_label}은 {target.date()}({self._weekday_ko(target)}) 입니다."
        grade, class_no, preferences = await self._legacy_get_user_info(platform, external_id)

        # Fetch all data concurrently
        schedule_text = await self._fetch_briefing_schedule(target.date(), date_label, req_id)
        weather_text = await self._fetch_briefing_weather(date_label, req_id)
        meal_text = await self._fetch_briefing_meal(target.date(), date_label, preferences, req_id)
        timetable_text = await self._fetch_briefing_timetable(
            target.date(), date_label, grade, class_no, req_id
        )

        return (
            [
                f"{header}\n\n{schedule_text}",
                weather_text,
                f"{meal_text}\n\n{timetable_text}",
            ],
            None,
            None,
        )

    def _get_briefing_target_date(self) -> tuple[datetime, str]:
        """Determine target date and label based on current time."""
        now = datetime.now(_KST)
        if now.time() >= time(17, 0):
            return now + timedelta(days=1), "내일"
        return now, "오늘"

    async def _fetch_briefing_schedule(self, target: date, date_label: str, req_id: str) -> str:
        """Fetch and format schedule text for briefing."""
        try:
            schedule_value = await asyncio.wait_for(
                self._fetch_schedule_text(target, req_id), timeout=2.0
            )
            if schedule_value and schedule_value != "일정이 없습니다.":
                return f"{date_label} 학사일정:\n{schedule_value}"
            return f"{date_label}은 학사일정이 없습니다."
        except (asyncio.TimeoutError, ConnectionError):
            return "학사일정 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요."

    async def _fetch_briefing_weather(self, date_label: str, req_id: str) -> str:
        """Fetch and format weather text for briefing."""
        return await self._weather_briefing_text(date_label, req_id)

    async def _fetch_briefing_meal(
        self, target: date, date_label: str, preferences: Dict[str, str], req_id: str
    ) -> str:
        """Fetch and format meal text for briefing."""
        try:
            meal_result = await asyncio.wait_for(
                self._build_meal_data(target, req_id), timeout=2.0
            )
        except (asyncio.TimeoutError, ConnectionError):
            return "급식 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요."

        if not isinstance(meal_result, dict):
            return "급식 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요."

        if "message" in meal_result:
            return meal_result["message"].replace(
                "등록된 데이터가 없습니다.", f"{date_label}은 급식을 실시하지 않습니다."
            )

        pref = preferences.get("AllergyInfo", "Number")
        formatted_menus = self._format_meal_menus(meal_result["menu"], pref)
        return f"{date_label} 급식:\n" + "\n".join(formatted_menus)

    def _format_meal_menus(self, menus: List[List], pref: str) -> List[str]:
        """Format meal menu items according to user preference."""
        formatted = []
        for name, allergies in menus:
            clean_name = name.replace("⭐", "")
            if pref == "None" or not allergies:
                formatted.append(clean_name)
            elif pref == "FullText":
                labels = [
                    _ALLERGY_LABELS[idx] for idx in allergies if 0 <= idx < len(_ALLERGY_LABELS)
                ]
                formatted.append(f"{clean_name}({', '.join(labels)})" if labels else clean_name)
            else:
                labels = [str(idx) for idx in allergies]
                formatted.append(f"{clean_name}({', '.join(labels)})" if labels else clean_name)
        return formatted

    async def _fetch_briefing_timetable(
        self, target: date, date_label: str, grade: int | None, class_no: int | None, req_id: str
    ) -> str:
        """Fetch and format timetable text for briefing."""
        if grade is None or class_no is None:
            return "등록된 사용자만 시간표를 볼 수 있습니다."

        try:
            tt_text = await asyncio.wait_for(
                self._build_timetable_text(grade, class_no, target, req_id), timeout=2.0
            )
            if tt_text == "등록된 데이터가 없습니다.":
                return "등록된 시간표가 없습니다."
            return f"{date_label} 시간표:\n" + tt_text.split("):\n", 1)[1]
        except (asyncio.TimeoutError, ConnectionError):
            return "시간표 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요."
        except Exception:
            return "등록된 사용자만 시간표를 볼 수 있습니다."

    async def _water_temperature(self, req_id: str) -> LegacyResponse:
        try:
            water_doc = await self._ensure_water_temperature()
            if not water_doc:
                raise ValueError
            hour_label = self._format_hour(water_doc.timestamp)
            message = (
                f"{water_doc.timestamp.date()} {hour_label} 측정자료:\n한강 수온은 {water_doc.temperature_c}°C 입니다."
            )
            return ([message], None, None)
        except ConnectionError:
            return (["한강 수온 서버에 연결하지 못했습니다.\n요청 ID: " + req_id], None, None)
        except ValueError:
            return (["측정소 또는 서버 오류입니다."], None, None)

    def _user_settings_card(self, platform: str, external_id: str, req_id: str) -> LegacyResponse:
        url = _settings.base_user_settings_url
        token = security.generate_token(
            "UserSettings",
            self._encode_identity(platform, external_id),
            ["GetUserInfo", "ManageUserInfo", "GetUsageData", "DeleteUsageData"],
            req_id,
        )
        return (
            [
                {
                    "type": "card",
                    "title": "내 정보 관리",
                    "body": "아래 버튼을 클릭해 관리 페이지로 접속해 주세요.\n링크는 10분 뒤 만료됩니다.",
                    "buttons": [
                        {
                            "type": "web",
                            "title": "내 정보 관리",
                            "url": f"{url}?token={token}",
                        }
                    ],
                }
            ],
            None,
            None,
        )

    async def _modify_user_info(
        self, platform: str, external_id: str, params: Dict[str, Any], req_id: str
    ) -> LegacyResponse:
        try:
            grade = int(params.get("grade"))
            class_no = int(params.get("class"))
        except (TypeError, ValueError):
            return (["올바른 숫자를 입력해 주세요."], None, None)

        if not (1 <= grade <= _settings.neis_num_grades and 1 <= class_no <= _settings.neis_num_classes):
            return (["올바른 학년/반을 입력해 주세요."], None, None)

        await self._user_service.update_user(platform, external_id, grade, class_no, None)
        return (["저장되었습니다."], None, None)

    async def _build_meal_data(self, target: date, req_id: str) -> Dict[str, Any]:
        meal_doc = await self._ensure_meal(target)
        if not meal_doc:
            return {"message": "등록된 데이터가 없습니다."}
        return {
            "date": self._format_legacy_date(target),
            "menu": [[item.name, item.allergies] for item in meal_doc.menus],
            "kcal": meal_doc.calories if meal_doc.calories is not None else 0,
        }

    async def _build_timetable_text(
        self, grade: int, class_no: int, target: date, req_id: str
    ) -> str:
        if target.weekday() >= 5:
            return "등록된 데이터가 없습니다."
        timetable = await self._ensure_timetable(target)
        if not timetable or not timetable.lessons:
            return "등록된 데이터가 없습니다."
        lessons = timetable.lessons.get(str(grade), {}).get(str(class_no))
        if not lessons:
            return "등록된 데이터가 없습니다."
        header = f"{grade}학년 {class_no}반,\n{target}({self._weekday_ko(target)}):"
        body = "".join([f"\n{idx + 1}교시: {subject}" for idx, subject in enumerate(lessons)])
        return header + body

    async def _fetch_schedule_text(self, target: date, req_id: str) -> str:
        schedule = await self._ensure_schedule(target)
        if schedule and schedule.summary:
            return schedule.summary
        return "일정이 없습니다."

    async def _build_schedule_range_message(
        self, start: datetime, end: datetime, req_id: str
    ) -> Tuple[str, str]:
        head: str
        if (end - start).days > 90:
            restricted_end = start + timedelta(days=90)
            head = (
                "서버 성능상의 이유로 최대 90일까지만 조회가 가능합니다.\n"
                f"조회기간이 {start.date()}부터 {restricted_end.date()}까지로 제한되었습니다.\n\n"
            )
            end = restricted_end
        else:
            head = f"{start.date()}부터 {end.date()}까지 조회합니다.\n\n"

        schedules = await self._data_service.get_schedules_in_range(start.date(), end.date())
        records: List[Tuple[int, int, int, str]] = []
        current = start.date()
        while current <= end.date():
            doc = schedules.get(current.isoformat())
            summary = doc.summary if doc and doc.summary else "일정이 없습니다."
            records.append((current.year, current.month, current.day, summary))
            current += timedelta(days=1)

        body = ""
        for content, group in self._group_by_content(records):
            segment = list(group)
            if segment[0] != segment[-1]:
                start_date = date(segment[0][0], segment[0][1], segment[0][2])
                end_date = date(segment[-1][0], segment[-1][1], segment[-1][2])
                body += (
                    f"{start_date}({self._weekday_ko(start_date)})~"
                    f"{end_date}({self._weekday_ko(end_date)}):\n{content}\n"
                )
            else:
                item_date = date(segment[0][0], segment[0][1], segment[0][2])
                body += f"{item_date}({self._weekday_ko(item_date)}):\n{content}\n"
        if not body:
            body = "일정이 없습니다.\n"
        return head, body[:-1]

    def _group_by_content(self, records: List[Tuple[int, int, int, str]]):
        current_content = None
        bucket: List[Tuple[int, int, int, str]] = []
        for item in records:
            if item[3] != current_content:
                if bucket:
                    yield current_content, bucket
                current_content = item[3]
                bucket = [item]
            else:
                bucket.append(item)
        if bucket:
            yield current_content, bucket

    async def _weather_briefing_text(self, date_label: str, req_id: str) -> str:
        try:
            weather_doc = await self._ensure_weather()
            if not weather_doc:
                raise ConnectionError
            return self._format_weather(date_label, weather_doc)
        except ConnectionError:
            return "날씨 서버에 연결하지 못했습니다.\n나중에 다시 시도해 보세요."

    async def _legacy_get_user_info(
        self, platform: str, external_id: str
    ) -> tuple[int | None, int | None, Dict[str, str]]:
        user = await self._user_service.ensure_user(platform, external_id)
        prefs = user.preferences.model_dump() if user.preferences else {}
        if "AllergyInfo" not in prefs:
            prefs["AllergyInfo"] = "Number"
        return user.grade, user.class_no, prefs

    @staticmethod
    def _encode_identity(platform: str, external_id: str) -> str:
        return f"{platform}:{external_id}"

    async def _ensure_meal(self, target: date) -> Optional[MealDocument]:
        meal = await self._data_service.get_meal(target)
        if meal:
            return meal
        try:
            await asyncio.wait_for(self._ingestion_service.sync_range(target, target), timeout=3.0)
            return await self._data_service.get_meal(target)
        except (asyncio.TimeoutError, ConnectionError):
            asyncio.create_task(self._ingestion_service.sync_range(target, target))
            return None

    async def _ensure_schedule(self, target: date) -> Optional[ScheduleDocument]:
        schedule = await self._data_service.get_schedule(target)
        if schedule:
            return schedule
        try:
            await asyncio.wait_for(self._ingestion_service.sync_range(target, target), timeout=3.0)
            return await self._data_service.get_schedule(target)
        except (asyncio.TimeoutError, ConnectionError):
            asyncio.create_task(self._ingestion_service.sync_range(target, target))
            return None

    async def _ensure_timetable(self, target: date) -> Optional[TimetableDocument]:
        timetable = await self._data_service.get_timetable(target)
        if timetable:
            return timetable
        try:
            await asyncio.wait_for(self._ingestion_service.sync_range(target, target), timeout=3.0)
            return await self._data_service.get_timetable(target)
        except (asyncio.TimeoutError, ConnectionError):
            asyncio.create_task(self._ingestion_service.sync_range(target, target))
            return None

    async def _ensure_weather(self):
        weather = await self._data_service.get_weather_recent()
        if weather:
            age = datetime.now(timezone.utc) - weather.timestamp
            if age <= timedelta(hours=_settings.cache_health_weather_ttl_hours):
                return weather
        try:
            await asyncio.wait_for(self._ingestion_service.sync_weather(), timeout=2.0)
            return await self._data_service.get_weather_recent()
        except (asyncio.TimeoutError, ConnectionError):
            asyncio.create_task(self._ingestion_service.sync_weather())
            return None

    async def _ensure_water_temperature(self):
        water = await self._data_service.get_water_temperature_recent()
        if water:
            age = datetime.now(timezone.utc) - water.timestamp
            if age <= timedelta(minutes=_settings.cache_health_water_temp_ttl_minutes):
                return water
        try:
            await asyncio.wait_for(self._ingestion_service.sync_water_temperature(), timeout=2.0)
            return await self._data_service.get_water_temperature_recent()
        except (asyncio.TimeoutError, ConnectionError):
            asyncio.create_task(self._ingestion_service.sync_water_temperature())
            return None

    def _format_weather(self, date_label: str, weather_doc) -> str:
        return (
            f"🌡️ {date_label} 최소/최대 기온: {weather_doc.temp_min}℃/{weather_doc.temp_max}℃\n\n"
            f"등굣길 예상 날씨: {weather_doc.sky}\n"
            f"🌡️ 기온: {weather_doc.temp}℃\n"
            f"🌦️ 강수 형태: {weather_doc.pty}\n"
            f"❔ 강수 확률: {weather_doc.precip_probability}%\n"
            f"💧 습도: {weather_doc.humidity}%"
        )

    @staticmethod
    def _format_legacy_date(target: date) -> str:
        names = ["월", "화", "수", "목", "금", "토", "일"]
        return f"{target:%Y-%m-%d}({names[target.weekday()]})"

    @staticmethod
    def _weekday_ko(target: date | datetime) -> str:
        names = ["월", "화", "수", "목", "금", "토", "일"]
        return names[target.weekday()]

    @staticmethod
    def _format_hour(timestamp: datetime) -> str:
        hour = timestamp.hour
        if hour == 0 or hour == 24:
            return "오전 12시"
        if hour < 12:
            return f"오전 {hour}시"
        if hour == 12:
            return "오후 12시"
        return f"오후 {hour - 12}시"

    def _extract_single_date(self, params: Dict[str, Any], req_id: str) -> Optional[date]:
        if "date" not in params:
            return None
        value = params["date"]
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                data = json.loads(value)
                if "date" in data:
                    return datetime.strptime(data["date"], "%Y-%m-%d").date()
            except (json.JSONDecodeError, ValueError):
                return None
        return None
