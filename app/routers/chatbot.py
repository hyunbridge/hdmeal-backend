from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ..config import get_settings
from ..dependencies import get_data_service, get_ingestion_service
from ..schemas.chatbot import KakaoSkillRequest, KakaoSkillResponse
from ..schemas.user_settings import (
    DeleteUserSettingsRequest,
    UpdateUserSettingsRequest,
    UserSettingsResponse,
)
from ..services.chatbot_service import ChatbotService
from ..services.data_service import DataService
from ..services.ingestion_service import IngestionService
from ..services.user_service import UserService
from ..utils import security

router = APIRouter()
_settings = get_settings()


def _extract_token(request: Request) -> str | None:
    return request.headers.get("X-HDMeal-Token") or request.query_params.get("token")


def _parse_identity(uid: str) -> tuple[str, str]:
    if ":" not in uid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="올바르지 않은 토큰입니다.")
    platform, external_id = uid.split(":", 1)
    if not platform or not external_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="올바르지 않은 토큰입니다.")
    return platform, external_id


@router.post("/skill/", response_model=KakaoSkillResponse)
async def handle_skill(
    payload: KakaoSkillRequest,
    request: Request,
    data_service: DataService = Depends(get_data_service),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
):
    req_id = getattr(request.state, "req_id", security.generate_req_id())

    token = _extract_token(request)
    if not token or not security.authorize_token(token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    chat_service = ChatbotService(data_service, ingestion_service)
    params = dict(payload.action.params)

    if "date" in params and params["date"]:
        try:
            params["date"] = datetime.strptime(json.loads(params["date"])["date"], "%Y-%m-%d")
        except (json.JSONDecodeError, KeyError, ValueError):
            params["date"] = None
    if "date_period" in params and params["date_period"]:
        try:
            parsed = json.loads(params["date_period"])
            params["date"] = [
                datetime.strptime(parsed["from"]["date"], "%Y-%m-%d"),
                datetime.strptime(parsed["to"]["date"], "%Y-%m-%d"),
            ]
        except (json.JSONDecodeError, KeyError, ValueError):
            params["date"] = None
        finally:
            params.pop("date_period", None)

    platform = "KT"
    external_id = payload.userRequest.user.id
    payloads, _, _ = await chat_service.handle_intent(
        platform=platform,
        external_id=external_id,
        intent=payload.intent.name,
        params=params,
        req_id=req_id,
    )
    kakao_outputs: List[dict] = []
    for item in payloads:
        if isinstance(item, str):
            kakao_outputs.append({"simpleText": {"text": item}})
        elif isinstance(item, dict):
            if item.get("type") == "card":
                card = {
                    "basicCard": {
                        "title": item.get("title", ""),
                        "description": item.get("body", ""),
                    }
                }
                if "image" in item:
                    card["basicCard"]["thumbnail"] = {"imageUrl": item["image"]}
                if "buttons" in item:
                    buttons = []
                    for button in item["buttons"]:
                        if button["type"] == "web":
                            buttons.append({
                                "action": "webLink",
                                "label": button["title"],
                                "webLinkUrl": button["url"],
                            })
                        elif button["type"] == "message":
                            buttons.append({
                                "action": "message",
                                "label": button["title"],
                                "messageText": button.get("postback", button["title"]),
                            })
                    if buttons:
                        card["basicCard"]["buttons"] = buttons
                kakao_outputs.append(card)
    return KakaoSkillResponse(template={"outputs": kakao_outputs})


@router.get(
    "/user/settings/",
    response_model=UserSettingsResponse,
    responses={
        200: {"description": "사용자 설정 조회 성공"},
        401: {"description": "토큰이 없거나 유효하지 않음"},
        403: {"description": "권한이 없음"},
    }
)
async def get_user_settings(
    request: Request,
    data_service: DataService = Depends(get_data_service),
):
    req_id = getattr(request.state, "req_id", security.generate_req_id())

    token = request.query_params.get("token") or request.headers.get("X-HDMeal-Token")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="토큰이 없습니다.")

    valid, uid_or_error, scope = security.validate_token(token, req_id)
    if not valid or not scope or "GetUserInfo" not in scope or uid_or_error is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="올바르지 않은 토큰입니다.")

    user_service = UserService(data_service)
    platform, external_id = _parse_identity(uid_or_error)
    user = await user_service.ensure_user(platform, external_id)

    return UserSettingsResponse(
        classes=list(range(1, _settings.neis_num_classes + 1)),
        grades=list(range(1, _settings.neis_num_grades + 1)),
        current_grade=user.grade,
        current_class=user.class_no,
        preferences=dict(user.preferences),
    )


@router.patch(
    "/user/settings/",
    responses={
        200: {"description": "사용자 설정 업데이트 성공"},
        400: {"description": "잘못된 학년/반 정보"},
        401: {"description": "토큰이 없음"},
        403: {"description": "권한이 없음"},
    }
)
async def patch_user_settings(
    payload: UpdateUserSettingsRequest,
    request: Request,
    data_service: DataService = Depends(get_data_service),
):
    req_id = getattr(request.state, "req_id", security.generate_req_id())

    token = request.headers.get("X-HDMeal-Token")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="토큰이 없습니다.")

    valid, identity, scope = security.validate_token(token, req_id)
    if not valid or not scope or "ManageUserInfo" not in scope or not identity:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="올바르지 않은 토큰입니다.")

    platform, external_id = _parse_identity(identity)

    if not (1 <= payload.user_grade <= _settings.neis_num_grades and 1 <= payload.user_class <= _settings.neis_num_classes):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="올바르지 않은 요청입니다.")

    user_service = UserService(data_service)
    await user_service.update_user(
        platform,
        external_id,
        payload.user_grade,
        payload.user_class,
        payload.preferences,
    )
    return {"message": "저장했습니다."}


@router.delete(
    "/user/settings/",
    responses={
        200: {"description": "사용자 정보 삭제 성공"},
        401: {"description": "토큰이 없음"},
        403: {"description": "권한이 없음"},
        404: {"description": "사용자 정보가 존재하지 않음"},
    }
)
async def delete_user_settings(
    request: Request,
    data_service: DataService = Depends(get_data_service),
):
    req_id = getattr(request.state, "req_id", security.generate_req_id())

    token = request.headers.get("X-HDMeal-Token")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="토큰이 없습니다.")

    valid, identity, scope = security.validate_token(token, req_id)
    if not valid or not scope or "ManageUserInfo" not in scope or not identity:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="올바르지 않은 토큰입니다.")

    platform, external_id = _parse_identity(identity)

    user_service = UserService(data_service)
    deleted = await user_service.delete_user(platform, external_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="사용자 정보가 없습니다.")
    return {"message": "삭제했습니다."}


@router.get("/cache/healthcheck/")
async def cache_healthcheck(
    request: Request,
    data_service: DataService = Depends(get_data_service),
):
    req_id = getattr(request.state, "req_id", security.generate_req_id())

    now = datetime.now(timezone.utc)
    timetable = await data_service.get_timetable(now.date())
    weather = await data_service.get_weather_recent()
    water = await data_service.get_water_temperature_recent()

    def _status_timetable():
        if not timetable:
            return "NotFound"
        created_at = getattr(timetable, "created_at", None)
        if created_at is None:
            return "NotFound"
        age = now - created_at
        ttl = _settings.cache_health_timetable_ttl_hours
        return "Valid" if age <= timedelta(hours=ttl) else "Expired"

    def _status_weather():
        if not weather:
            return "NotFound"
        age = now - weather.timestamp
        ttl = _settings.cache_health_weather_ttl_hours
        return "Valid" if age <= timedelta(hours=ttl) else "Expired"

    def _status_water():
        if not water:
            return "NotFound"
        age = now - water.timestamp
        ttl = _settings.cache_health_water_temp_ttl_minutes
        return "Valid" if age <= timedelta(minutes=ttl) else "Expired"

    return {
        "timetable": _status_timetable(),
        "weather": _status_weather(),
        "water_temperature": _status_water(),
    }
