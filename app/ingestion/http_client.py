from __future__ import annotations

import asyncio
import random
from typing import Dict, Iterable, Optional

import httpx

from ..utils.logging import get_logger

logger = get_logger("ingestion.http")

_DEFAULT_LIMITS = httpx.Limits(max_keepalive_connections=10, max_connections=20)
_DEFAULT_RETRY_STATUSES = {429, 500, 502, 503, 504}

_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(limits=_DEFAULT_LIMITS)
    return _client


async def close_http_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _compute_delay(attempt: int, base_delay: float, retry_after: Optional[str]) -> float:
    if retry_after:
        try:
            retry_after_value = float(retry_after)
        except ValueError:
            retry_after_value = 0.0
        else:
            return max(0.0, retry_after_value)
    backoff = base_delay * (2 ** attempt)
    jitter = random.uniform(0, base_delay)
    return backoff + jitter


async def get_json(
    url: str,
    params: Optional[Dict[str, str]] = None,
    *,
    timeout: float | httpx.Timeout = 10.0,
    retries: int = 2,
    backoff: float = 0.5,
    retry_statuses: Optional[Iterable[int]] = None,
    label: Optional[str] = None,
) -> Dict:
    client = get_http_client()
    statuses = set(retry_statuses or _DEFAULT_RETRY_STATUSES)
    request_label = label or url

    for attempt in range(retries + 1):
        try:
            response = await client.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if attempt < retries and status in statuses:
                delay = _compute_delay(attempt, backoff, exc.response.headers.get("Retry-After"))
                logger.warning(
                    "%s request failed with status %s (attempt %s/%s); retrying in %.2fs",
                    request_label,
                    status,
                    attempt + 1,
                    retries + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("%s request failed with status %s after %s attempts", request_label, status, attempt + 1)
            raise
        except (httpx.RequestError, ValueError) as exc:
            if attempt < retries:
                delay = _compute_delay(attempt, backoff, None)
                logger.warning(
                    "%s request error (attempt %s/%s): %s; retrying in %.2fs",
                    request_label,
                    attempt + 1,
                    retries + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("%s request failed after %s attempts: %s", request_label, attempt + 1, exc)
            raise

    return {}
