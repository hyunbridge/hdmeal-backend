from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from ..schemas.data import UserDocument, UserPreferences
from .data_service import DataService


def utc_now() -> datetime:
    """Get current UTC time (replacement for deprecated datetime.utcnow())."""
    return datetime.now(timezone.utc)

_VALID_PREFERENCE_VALUES = {
    "AllergyInfo": {"None", "Number", "FullText"},
}


class UserService:
    def __init__(self, data_service: DataService):
        self._data_service = data_service

    async def get_user(self, platform: str, external_id: str) -> UserDocument | None:
        return await self._data_service.get_user(platform, external_id)

    async def ensure_user(self, platform: str, external_id: str) -> UserDocument:
        existing = await self.get_user(platform, external_id)
        if existing:
            return existing
        return await self._data_service.upsert_user(
            UserDocument(platform=platform, external_id=external_id)
        )

    async def update_user(
        self,
        platform: str,
        external_id: str,
        grade: Optional[int],
        class_no: Optional[int],
        preferences: Optional[Dict[str, str]] = None,
    ) -> UserDocument:
        doc = await self.ensure_user(platform, external_id)
        payload = doc.model_copy(update={
            "grade": grade,
            "class_no": class_no,
            "updated_at": utc_now(),
        })
        if preferences:
            prefs = dict(doc.preferences)
            for key, value in preferences.items():
                if key in _VALID_PREFERENCE_VALUES and value in _VALID_PREFERENCE_VALUES[key]:
                    prefs[key] = value
            payload = payload.model_copy(update={"preferences": UserPreferences(**prefs)})
        return await self._data_service.upsert_user(payload)

    async def delete_user(self, platform: str, external_id: str) -> bool:
        return await self._data_service.delete_user(platform, external_id)
