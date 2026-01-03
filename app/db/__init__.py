from __future__ import annotations

from datetime import timezone
from typing import AsyncGenerator

from pymongo.asynchronous.client import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from ..config import get_settings

_settings = get_settings()
_client: AsyncMongoClient | None = None


def get_client() -> AsyncMongoClient:
    """MongoDB 클라이언트 인스턴스를 반환합니다."""

    global _client
    if _client is None:
        _client = AsyncMongoClient(
            _settings.mongodb_uri,
            tz_aware=True,
            tzinfo=timezone.utc,
        )
    return _client


def get_database() -> AsyncDatabase:
    """설정된 기본 데이터베이스 핸들을 반환합니다."""

    return get_client()[_settings.mongodb_db]


async def get_db() -> AsyncGenerator[AsyncDatabase, None]:
    """FastAPI Depends용 MongoDB 의존성."""

    yield get_database()


def close_client() -> None:
    """MongoDB 클라이언트를 종료합니다."""

    global _client
    if _client is None:
        return
    _client.close()
    _client = None
