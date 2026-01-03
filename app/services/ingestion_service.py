from __future__ import annotations

from datetime import date, timedelta

from .data_service import DataService
from ..ingestion import auxiliary, neis
from ..utils.logging import get_logger

logger = get_logger("ingestion")


class IngestionService:
    def __init__(self, data_service: DataService):
        self._data_service = data_service

    async def sync_window(self, center: date | None = None, days_before: int = 10, days_after: int = 10) -> None:
        target = center or date.today()
        start = target - timedelta(days=days_before)
        end = target + timedelta(days=days_after)
        logger.info(f"Syncing data window: {start} to {end}")
        await self.sync_range(start, end)

    async def sync_range(self, start: date, end: date) -> None:
        try:
            logger.debug(f"Fetching NEIS data for range: {start} to {end}")
            dataset = await neis.fetch_all(start, end)

            meal_count = len(dataset["meal"])
            schedule_count = len(dataset["schedule"])
            timetable_count = len(dataset["timetable"])

            for meal in dataset["meal"].values():
                await self._data_service.upsert_meal(meal)
            for schedule in dataset["schedule"].values():
                await self._data_service.upsert_schedule(schedule)
            for timetable in dataset["timetable"].values():
                await self._data_service.upsert_timetable(timetable)

            logger.info(
                f"Synced {meal_count} meals, {schedule_count} schedules, "
                f"{timetable_count} timetables for {start} to {end}"
            )
        except Exception as e:
            logger.error(f"Failed to sync range {start} to {end}: {e}")
            raise

    async def sync_weather(self) -> None:
        try:
            logger.debug("Fetching weather data")
            weather = await auxiliary.fetch_weather()
            if weather:
                await self._data_service.upsert_weather(weather)
                logger.info(f"Weather data synced: {weather.timestamp}")
            else:
                logger.warning("No weather data available")
        except Exception as e:
            logger.error(f"Failed to sync weather: {e}")
            raise

    async def sync_water_temperature(self) -> None:
        try:
            logger.debug("Fetching water temperature data")
            water = await auxiliary.fetch_water_temperature()
            if water:
                await self._data_service.upsert_water_temperature(water)
                logger.info(f"Water temperature synced: {water.temperature_c}°C at {water.timestamp}")
            else:
                logger.warning("No water temperature data available")
        except Exception as e:
            logger.error(f"Failed to sync water temperature: {e}")
            raise
