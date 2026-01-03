from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel


class KakaoUser(BaseModel):
    id: str


class KakaoUserRequest(BaseModel):
    user: KakaoUser
    utterance: Optional[str] = None


class KakaoIntent(BaseModel):
    name: str


class KakaoAction(BaseModel):
    params: Dict[str, Any] = {}


class KakaoSkillRequest(BaseModel):
    userRequest: KakaoUserRequest
    intent: KakaoIntent
    action: KakaoAction


class KakaoSkillResponse(BaseModel):
    version: str = "2.0"
    template: Dict[str, Any]
