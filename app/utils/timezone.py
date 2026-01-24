from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(_KST)


def today_kst() -> date:
    return now_kst().date()


def to_kst(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=_KST)
    return value.astimezone(_KST)
