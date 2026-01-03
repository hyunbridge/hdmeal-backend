from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


def utc_now() -> datetime:
    """Get current UTC time (replacement for deprecated datetime.utcnow())."""
    return datetime.now(timezone.utc)


class MealMenuItem(BaseModel):
    name: str
    allergies: List[int] = Field(default_factory=list)


class MealDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
    date: date
    menus: List[MealMenuItem] = Field(default_factory=list)
    menus_plain: List[str] = Field(default_factory=list)
    calories: Optional[float] = None
    source_hash: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now)


class ScheduleEntry(BaseModel):
    name: str
    grades: List[int] = Field(default_factory=list)


class ScheduleDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
    date: date
    entries: Optional[List[ScheduleEntry]] = None
    summary: Optional[str] = None
    created_at: datetime = Field(default_factory=utc_now)


class TimetableDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
    date: date
    lessons: Dict[str, Dict[str, List[str]]] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class WeatherDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    timestamp: datetime
    temp: str
    temp_min: str
    temp_max: str
    sky: str
    pty: str
    precip_probability: str
    humidity: str
    first_hour: str
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="before")
    @classmethod
    def _normalize_id(cls, values):
        if isinstance(values, dict) and values.get("_id") is not None:
            values["_id"] = str(values["_id"])
        return values


class WaterTemperatureDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    timestamp: datetime
    temperature_c: float
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="before")
    @classmethod
    def _normalize_id(cls, values):
        if isinstance(values, dict) and values.get("_id") is not None:
            values["_id"] = str(values["_id"])
        return values


class UserPreferences(BaseModel):
    AllergyInfo: str = "Number"


class UserDocument(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    platform: str
    external_id: str
    grade: Optional[int] = None
    class_no: Optional[int] = None
    preferences: UserPreferences = Field(default_factory=UserPreferences)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="before")
    @classmethod
    def _normalise_id(cls, values):
        if isinstance(values, dict) and values.get("_id") is not None:
            values["_id"] = str(values["_id"])
        return values
