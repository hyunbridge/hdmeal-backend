from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from ..config import get_settings
from ..dependencies import get_data_service, get_ingestion_service
from ..services.data_service import DataService
from ..services.ingestion_service import IngestionService
from ..utils import security
from ..schemas import (
    DayOut,
    DayResponse,
    DaysResponse,
    MealItemOut,
    MealOut,
    MetaResponse,
    RangeMeta,
    ScheduleItemOut,
    TimetableOut,
)

router = APIRouter(prefix="/api/app", tags=["app"])
_settings = get_settings()
_DEFAULT_PAST_DAYS = 1
_DEFAULT_FUTURE_DAYS = 7


def _parse_date(value: Optional[str], field: str) -> date:
    """필수 날짜 파라미터를 파싱하고 검증합니다."""

    if not value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'{field}' 파라미터가 필요합니다.",
        )
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'{field}' 형식이 올바르지 않습니다. (YYYY-MM-DD)",
        ) from None


def _parse_optional_date(value: Optional[str]) -> Optional[date]:
    """선택적 날짜 파라미터를 파싱합니다."""

    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="잘못된 날짜 형식입니다. (YYYY-MM-DD)",
        ) from None


def _validate_range(start_date: date, end_date: date) -> None:
    """조회 가능한 날짜 범위를 검증합니다."""

    if start_date > end_date:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="시작일이 종료일보다 늦습니다.")
    if (end_date - start_date).days + 1 > _settings.max_days_range:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"최대 조회 기간은 {_settings.max_days_range}일입니다.",
        )


def _serialize_meal(meal) -> Optional[MealOut]:
    """급식 도메인 모델을 응답 스키마로 변환합니다."""

    if not meal or not meal.menus:
        return None
    return MealOut(
        items=[MealItemOut(name=item.name, allergies=item.allergies) for item in meal.menus],
        kcal=meal.calories,
        updatedAt=getattr(meal, "created_at", None),
    )


def _serialize_schedule(schedule) -> List[ScheduleItemOut]:
    """학사 일정 목록을 응답 스키마로 변환합니다."""

    if not schedule or not schedule.entries:
        return []
    return [ScheduleItemOut(name=entry.name, grades=entry.grades) for entry in schedule.entries]


def _serialize_timetable(data_service: DataService, timetable) -> TimetableOut:
    """시간표 도메인 모델을 응답 스키마로 변환합니다."""

    lessons = timetable.lessons if timetable else data_service.empty_timetable()
    updated_at = getattr(timetable, "created_at", None) if timetable else None
    return TimetableOut(lessons=lessons, updatedAt=updated_at)


def _serialize_day(
    target_date: date,
    data_service: DataService,
    meal,
    schedule,
    timetable,
) -> DayOut:
    """단일 일자의 통합 데이터를 구성합니다."""

    return DayOut(
        event_date=target_date,
        meal=_serialize_meal(meal),
        schedule=_serialize_schedule(schedule),
        timetable=_serialize_timetable(data_service, timetable),
    )


@router.get(
    "/days",
    response_model=DaysResponse,
    response_model_exclude_none=True,
    summary="기간별 통합 데이터 조회",
    description="지정된 기간(기본값: 어제부터 7일 후까지)의 급식, 학사일정, 시간표 데이터를 반환합니다.",
    responses={
        200: {
            "description": "성공적으로 데이터를 조회했습니다",
            "content": {
                "application/json": {
                    "example": {
                        "requestId": "ABC123",
                        "range": {"from": "2024-01-01", "to": "2024-01-07"},
                        "data": []
                    }
                }
            }
        },
        400: {"description": "잘못된 날짜 형식 또는 범위 초과"},
        500: {"description": "서버 내부 오류"},
    },
)
async def get_days(
    request: Request,
    response: Response,
    start: Optional[str] = Query(default=None, alias="from"),
    end: Optional[str] = Query(default=None, alias="to"),
    data_service: DataService = Depends(get_data_service),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> DaysResponse:
    """일자 범위에 대한 통합 데이터를 조회합니다."""
    request_id = getattr(request.state, "req_id", security.generate_req_id())

    today = date.today()
    start_date = _parse_optional_date(start) or (today - timedelta(days=_DEFAULT_PAST_DAYS))
    end_date = _parse_optional_date(end) or (today + timedelta(days=_DEFAULT_FUTURE_DAYS))
    _validate_range(start_date, end_date)

    await ingestion_service.sync_range(start_date, end_date)

    meals_task = data_service.get_meals_in_range(start_date, end_date)
    schedules_task = data_service.get_schedules_in_range(start_date, end_date)
    timetables_task = data_service.get_timetables_in_range(start_date, end_date)
    meals, schedules, timetables = await asyncio.gather(meals_task, schedules_task, timetables_task)

    days: List[DayOut] = []
    current = start_date
    while current <= end_date:
        days.append(
            _serialize_day(
                current,
                data_service,
                meals.get(current.isoformat()),
                schedules.get(current.isoformat()),
                timetables.get(current.isoformat()),
            )
        )
        current += timedelta(days=1)

    response.headers["X-HDMeal-Range"] = f"{start_date.isoformat()}~{end_date.isoformat()}"
    return DaysResponse(
        requestId=request_id,
        range=RangeMeta(from_=start_date, to=end_date),
        data=days,
    )


@router.get(
    "/days/{day}",
    response_model=DayResponse,
    response_model_exclude_none=True,
    summary="단일 일자 통합 데이터 조회",
    description="특정 일자의 급식, 학사일정, 시간표 데이터를 조회합니다.",
    responses={
        200: {"description": "성공적으로 데이터를 조회했습니다"},
        400: {"description": "잘못된 날짜 형식 (YYYY-MM-DD 형식 필요)"},
        500: {"description": "서버 내부 오류"},
    },
)
async def get_day(
    day: str,
    request: Request,
    response: Response,
    data_service: DataService = Depends(get_data_service),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> DayResponse:
    """단일 일자에 대한 통합 데이터를 반환합니다."""
    request_id = getattr(request.state, "req_id", security.generate_req_id())

    target = _parse_date(day, "day")
    await ingestion_service.sync_range(target, target)

    meal, schedule, timetable = await asyncio.gather(
        data_service.get_meal(target),
        data_service.get_schedule(target),
        data_service.get_timetable(target),
    )

    response.headers["X-HDMeal-Range"] = f"{target.isoformat()}~{target.isoformat()}"
    return DayResponse(
        requestId=request_id,
        data=_serialize_day(target, data_service, meal, schedule, timetable),
    )


@router.get(
    "/meta",
    response_model=MetaResponse,
    summary="모바일 앱 메타 정보",
    description="모바일 앱이 활용하는 최신 버전 및 빌드 정보를 제공합니다.",
    responses={
        200: {
            "description": "메타 정보 조회 성공",
            "content": {
                "application/json": {
                    "example": {
                        "requestId": "ABC123",
                        "data": {
                            "version": "1.0.0",
                            "build": 1,
                            "debug": False
                        }
                    }
                }
            }
        }
    },
)
async def get_meta(request: Request) -> MetaResponse:
    """모바일 애플리케이션용 메타데이터를 조회합니다."""

    request_id = getattr(request.state, "req_id", security.generate_req_id())
    return MetaResponse(
        requestId=request_id,
        data={
            "version": _settings.api_version,
            "build": _settings.api_build,
            "debug": _settings.debug,
        },
    )
