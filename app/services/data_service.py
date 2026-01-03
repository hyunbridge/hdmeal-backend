from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from pymongo import ASCENDING, ReturnDocument
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase

from ..config import get_settings
from ..schemas.data import (
    MealDocument,
    ScheduleDocument,
    TimetableDocument,
    UserDocument,
    UserPreferences,
    WaterTemperatureDocument,
    WeatherDocument,
)


def utc_now() -> datetime:
    """Get current UTC time (replacement for deprecated datetime.utcnow())."""
    return datetime.now(timezone.utc)


def _range_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """Convert inclusive [start, end] date range to UTC datetime bounds."""

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    end_exclusive = datetime.combine(
        end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )
    return start_dt, end_exclusive


class DataService:
    """MongoDB 접근을 캡슐화하여 도메인 데이터를 관리합니다."""

    _indexes_ready: bool = False
    _index_lock: asyncio.Lock | None = None

    def __init__(self, database: AsyncIOMotorDatabase):
        self._db = database
        self._meals: AsyncIOMotorCollection = database["meals"]
        self._schedules: AsyncIOMotorCollection = database["schedules"]
        self._timetables: AsyncIOMotorCollection = database["timetables"]
        self._weather: AsyncIOMotorCollection = database["weather"]
        self._water_temperature: AsyncIOMotorCollection = database["water_temperatures"]
        self._users: AsyncIOMotorCollection = database["users"]
        self._settings = get_settings()

    async def ensure_indexes(self) -> None:
        """컬렉션 인덱스를 최초 1회만 생성합니다."""

        if type(self)._indexes_ready:
            return

        if type(self)._index_lock is None:
            type(self)._index_lock = asyncio.Lock()

        async with type(self)._index_lock:  # type: ignore[arg-type]
            if type(self)._indexes_ready:
                return
            await asyncio.gather(
                self._meals.create_index("date", unique=True),
                self._schedules.create_index("date", unique=True),
                self._timetables.create_index("date", unique=True),
                self._weather.create_index("timestamp", unique=True),
                self._water_temperature.create_index("timestamp", unique=True),
            )
            await self._users.create_index(
                [("platform", ASCENDING), ("external_id", ASCENDING)], unique=True
            )
            type(self)._indexes_ready = True

    # ------------------------------------------------------------------
    # Meals
    # ------------------------------------------------------------------
    async def upsert_meal(self, document: MealDocument) -> MealDocument:
        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"_id", "created_at"}
        )
        payload["date"] = datetime.combine(document.date, datetime.min.time(), tzinfo=timezone.utc)
        await self._meals.update_one(
            {"_id": document.id},
            {"$set": payload, "$setOnInsert": {"created_at": utc_now()}},
            upsert=True,
        )
        return await self.get_meal(document.date)

    async def get_meal(self, target: date) -> Optional[MealDocument]:
        """특정 일자의 급식 정보를 조회합니다."""

        key = target.isoformat()
        data = await self._meals.find_one({"_id": key})
        return MealDocument.model_validate(data) if data else None

    async def get_meals_in_range(self, start: date, end: date) -> Dict[str, MealDocument]:
        """기간 내 급식 정보를 조회하여 일자별로 매핑합니다."""

        start_dt, end_exclusive = _range_bounds(start, end)
        cursor = self._meals.find({"date": {"$gte": start_dt, "$lt": end_exclusive}})
        results: Dict[str, MealDocument] = {}
        async for item in cursor:
            doc = MealDocument.model_validate(item)
            results[doc.date.isoformat()] = doc
        return results

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------
    async def upsert_schedule(self, document: ScheduleDocument) -> ScheduleDocument:
        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"_id", "created_at"}
        )
        payload["date"] = datetime.combine(document.date, datetime.min.time(), tzinfo=timezone.utc)
        await self._schedules.update_one(
            {"_id": document.id},
            {"$set": payload, "$setOnInsert": {"created_at": utc_now()}},
            upsert=True,
        )
        return await self.get_schedule(document.date)

    async def get_schedule(self, target: date) -> Optional[ScheduleDocument]:
        """특정 일자의 학사일정을 조회합니다."""

        data = await self._schedules.find_one({"_id": target.isoformat()})
        return ScheduleDocument.model_validate(data) if data else None

    async def get_schedules_in_range(self, start: date, end: date) -> Dict[str, ScheduleDocument]:
        """기간 내 학사일정을 조회하여 일자별로 매핑합니다."""

        start_dt, end_exclusive = _range_bounds(start, end)
        cursor = self._schedules.find({"date": {"$gte": start_dt, "$lt": end_exclusive}})
        results: Dict[str, ScheduleDocument] = {}
        async for item in cursor:
            doc = ScheduleDocument.model_validate(item)
            results[doc.date.isoformat()] = doc
        return results

    # ------------------------------------------------------------------
    # Timetable
    # ------------------------------------------------------------------
    async def upsert_timetable(self, document: TimetableDocument) -> TimetableDocument:
        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"_id", "created_at"}
        )
        payload["date"] = datetime.combine(document.date, datetime.min.time(), tzinfo=timezone.utc)
        await self._timetables.update_one(
            {"_id": document.id},
            {"$set": payload, "$setOnInsert": {"created_at": utc_now()}},
            upsert=True,
        )
        return await self.get_timetable(document.date)

    async def get_timetable(self, target: date) -> Optional[TimetableDocument]:
        """특정 일자의 시간표를 조회합니다."""

        data = await self._timetables.find_one({"_id": target.isoformat()})
        return TimetableDocument.model_validate(data) if data else None

    async def get_timetables_in_range(self, start: date, end: date) -> Dict[str, TimetableDocument]:
        """기간 내 시간표를 조회하여 일자별로 매핑합니다."""

        start_dt, end_exclusive = _range_bounds(start, end)
        cursor = self._timetables.find({"date": {"$gte": start_dt, "$lt": end_exclusive}})
        results: Dict[str, TimetableDocument] = {}
        async for item in cursor:
            doc = TimetableDocument.model_validate(item)
            results[doc.date.isoformat()] = doc
        return results

    # ------------------------------------------------------------------
    # Weather & water temperature
    # ------------------------------------------------------------------
    async def upsert_weather(self, document: WeatherDocument) -> WeatherDocument:
        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"_id", "created_at"}
        )
        await self._weather.update_one(
            {"timestamp": document.timestamp},
            {"$set": payload, "$setOnInsert": {"created_at": utc_now()}},
            upsert=True,
        )
        stored = await self._weather.find_one({"timestamp": document.timestamp})
        return WeatherDocument.model_validate(stored)

    async def get_weather_recent(self) -> Optional[WeatherDocument]:
        """가장 최근의 날씨 정보를 반환합니다."""

        data = await self._weather.find().sort("timestamp", -1).limit(1).to_list(length=1)
        if not data:
            return None
        return WeatherDocument.model_validate(data[0])

    async def upsert_water_temperature(self, document: WaterTemperatureDocument) -> WaterTemperatureDocument:
        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"_id", "created_at"}
        )
        await self._water_temperature.update_one(
            {"timestamp": document.timestamp},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": utc_now()},
            },
            upsert=True,
        )
        stored = await self._water_temperature.find_one({"timestamp": document.timestamp})
        return WaterTemperatureDocument.model_validate(stored)

    async def get_water_temperature_recent(self) -> Optional[WaterTemperatureDocument]:
        """가장 최근의 한강 수온 정보를 반환합니다."""

        data = await self._water_temperature.find().sort("timestamp", -1).limit(1).to_list(length=1)
        if not data:
            return None
        return WaterTemperatureDocument.model_validate(data[0])

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------
    async def get_user(self, platform: str, external_id: str) -> Optional[UserDocument]:
        """사용자 정보를 조회합니다."""

        data = await self._users.find_one({"platform": platform, "external_id": external_id})
        return UserDocument.model_validate(data) if data else None

    async def upsert_user(self, document: UserDocument) -> UserDocument:
        """사용자 정보를 생성 또는 업데이트합니다."""

        payload = document.model_dump(
            by_alias=True, exclude_none=True, exclude={"id", "_id", "created_at"}
        )
        payload["updated_at"] = utc_now()
        filter_query = {"platform": document.platform, "external_id": document.external_id}
        result = await self._users.find_one_and_update(
            filter_query,
            {
                "$set": payload,
                # Only populate immutable metadata on insert to avoid key conflicts
                "$setOnInsert": {"created_at": utc_now()},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if result is None:
            result = await self._users.find_one(filter_query)
        return UserDocument.model_validate(result) if result else document

    async def update_user_preferences(
        self, platform: str, external_id: str, preferences: Dict[str, str]
    ) -> Optional[UserDocument]:
        """사용자 선호 설정을 갱신합니다."""

        existing = await self.get_user(platform, external_id)
        if not existing:
            existing = UserDocument(platform=platform, external_id=external_id)
        prefs = dict(existing.preferences)
        prefs.update(preferences)
        document = existing.model_copy(update={"preferences": UserPreferences(**prefs)})
        return await self.upsert_user(document)

    async def delete_user(self, platform: str, external_id: str) -> bool:
        """사용자 정보를 삭제합니다."""

        result = await self._users.delete_one({"platform": platform, "external_id": external_id})
        return result.deleted_count > 0

    # ------------------------------------------------------------------
    # Aggregated payload for mobile app
    # ------------------------------------------------------------------
    async def generate_app_payload(self, version: str, start: date, end: date) -> Dict[str, Dict[str, object]]:
        """레거시 앱 호환을 위한 페이로드를 생성합니다."""

        meals, schedules, timetables = await asyncio.gather(
            self.get_meals_in_range(start, end),
            self.get_schedules_in_range(start, end),
            self.get_timetables_in_range(start, end),
        )

        payload: Dict[str, Dict[str, object]] = {}
        current = start
        while current <= end:
            key = current.isoformat()
            meal_doc = meals.get(key)
            schedule_doc = schedules.get(key)
            timetable_doc = timetables.get(key)

            payload[key] = {
                "Meal": self._format_meal(version, meal_doc),
                "Schedule": self._format_schedule(version, schedule_doc),
                "Timetable": timetable_doc.lessons if timetable_doc else self._empty_timetable(),
            }
            current += timedelta(days=1)
        return payload

    def _format_meal(self, version: str, meal: Optional[MealDocument]) -> List[object]:
        """버전별 급식 응답 포맷을 생성합니다."""

        if not meal or not meal.menus:
            return [None, None]
        if version == "v2":
            menus = [item.name for item in meal.menus]
        else:
            menus = [[item.name, item.allergies] for item in meal.menus]
        return [menus, meal.calories]

    def _format_schedule(self, version: str, schedule: Optional[ScheduleDocument]) -> object:
        """버전별 학사일정 응답 포맷을 생성합니다."""

        if not schedule:
            return None if version == "v4" else "일정이 없습니다."
        if version == "v4":
            if not schedule.entries:
                return []
            return [[entry.name, entry.grades] for entry in schedule.entries]
        return schedule.summary or "일정이 없습니다."

    def empty_timetable(self) -> Dict[str, Dict[str, List[str]]]:
        """학년/반 구조를 유지하는 빈 시간표를 생성합니다."""

        return self._empty_timetable()

    def _empty_timetable(self) -> Dict[str, Dict[str, List[str]]]:
        timetable: Dict[str, Dict[str, List[str]]] = {}
        for grade in range(1, self._settings.neis_num_grades + 1):
            classes: Dict[str, List[str]] = {}
            for class_no in range(1, self._settings.neis_num_classes + 1):
                classes[str(class_no)] = []
            timetable[str(grade)] = classes
        return timetable
