from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class MealItemOut(BaseModel):
    name: str = Field(description="메뉴 이름")
    allergies: List[int] = Field(default_factory=list, description="알레르기 표시 번호 목록")


class MealOut(BaseModel):
    items: List[MealItemOut] = Field(description="알레르기 정보를 포함한 메뉴 목록")
    kcal: Optional[float] = Field(default=None, description="총 열량(kcal)")
    updatedAt: Optional[datetime] = Field(default=None, description="데이터 최신화 시각 (UTC)")


class ScheduleItemOut(BaseModel):
    name: str = Field(description="학사 일정명")
    grades: List[int] = Field(default_factory=list, description="해당 일정이 적용되는 학년 목록")


class TimetableOut(BaseModel):
    lessons: Dict[str, Dict[str, List[str]]] = Field(
        description="학년/반별 시간표 정보", example={"1": {"1": ["국어", "수학"]}}
    )
    updatedAt: Optional[datetime] = Field(default=None, description="시간표 데이터 최신화 시각 (UTC)")


class DayOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_date: date = Field(alias="date", description="조회 일자")
    meal: Optional[MealOut] = Field(default=None, description="급식 정보")
    schedule: List[ScheduleItemOut] = Field(default_factory=list, description="학사 일정 목록")
    timetable: TimetableOut = Field(description="시간표 정보")


class RangeMeta(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: date = Field(alias="from", description="조회 시작일")
    to: date = Field(description="조회 종료일")


class DaysResponse(BaseModel):
    requestId: str = Field(description="요청 식별자")
    range: RangeMeta = Field(description="조회 구간 정보")
    data: List[DayOut] = Field(description="일자별 통합 데이터 목록")


class DayResponse(BaseModel):
    requestId: str = Field(description="요청 식별자")
    data: DayOut = Field(description="단일 일자 통합 데이터")


class MetaResponse(BaseModel):
    requestId: str = Field(description="요청 식별자")
    data: Dict[str, object] = Field(description="버전 및 운영 정보")
