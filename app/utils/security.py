from __future__ import annotations

import datetime
import random
from typing import List, Tuple

import httpx
from authlib.jose import JsonWebToken, JWTClaims
from authlib.jose.errors import ExpiredTokenError, JoseError

from ..config import get_settings
from . import base58

_settings = get_settings()
_jwt = JsonWebToken(["HS256"])

_claim_options = {
    "iss": {"essential": True, "values": ["HDMeal-UserSettings"]},
    "uid": {"essential": True},
    "scope": {"essential": True},
    "reqId": {"essential": True},
    "nbf": {"essential": True, "validate": JWTClaims.validate_nbf},
    "exp": {"essential": True, "validate": JWTClaims.validate_exp},
}


def generate_req_id() -> str:
    timestamp = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 100))
    random_part = str(random.randint(1, 9))
    for _ in range(16 - len(timestamp)):
        random_part += str(random.randint(0, 9))
    timestamp_position = str(len(random_part))
    checksum = str(int(random_part + timestamp + timestamp_position) % 997).zfill(3)
    raw = int(random_part + timestamp + timestamp_position + checksum)
    return base58.encode(raw).zfill(12)


def authorize_token(token: str) -> bool:
    return token in _settings.auth_tokens


def generate_token(issuer: str, uid: str, scope: List[str], req_id: str) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    claims = {
        "iss": f"HDMeal-{issuer}",
        "uid": uid,
        "scope": scope,
        "reqId": req_id,
        "nbf": now,
        "exp": now + datetime.timedelta(minutes=10),
    }
    header = {"alg": "HS256", "typ": "JWT"}
    token = _jwt.encode(header, claims, _settings.jwt_secret)
    return token.decode()


def validate_token(token: str, req_id: str) -> Tuple[bool, str | None, List[str] | None]:
    try:
        decoded = _jwt.decode(token, _settings.jwt_secret, claims_options=_claim_options)
        decoded.validate()
    except ExpiredTokenError:
        return False, "ExpiredToken", None
    except JoseError:
        return False, "InvalidToken", None
    return True, decoded["uid"], decoded.get("scope", [])


def validate_recaptcha(token: str, req_id: str) -> Tuple[bool, str | None]:
    if not _settings.recaptcha_secret:
        return False, "RecaptchaTokenValidationError"
    params = {"secret": _settings.recaptcha_secret, "response": token}
    try:
        response = httpx.post("https://www.google.com/recaptcha/api/siteverify", data=params, timeout=5.0)
    except httpx.HTTPError:
        return False, "RecaptchaTokenValidationError"
    result = response.json()
    if result.get("success"):
        return True, None
    return False, "InvalidRecaptchaToken"
