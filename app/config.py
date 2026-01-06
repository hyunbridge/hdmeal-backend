from functools import lru_cache
from typing import List
from urllib.parse import urlsplit

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "HDMeal Unified Backend"
    debug: bool = False
    api_version: str = Field(default="1.0.0", alias="HDMeal_AppVersion")
    api_build: int = Field(default=1, alias="HDMeal_AppBuild")

    mongodb_uri: str = Field(..., alias="MONGODB_URI")
    mongodb_db: str = Field(..., alias="MONGODB_DATABASE")

    neis_api_key: str = Field(..., alias="NEIS_OPENAPI_TOKEN")
    neis_atpt_code: str = Field(..., alias="ATPT_OFCDC_SC_CODE")
    neis_school_code: str = Field(..., alias="SD_SCHUL_CODE")
    neis_num_grades: int = Field(..., alias="NUM_OF_GRADES")
    neis_num_classes: int = Field(..., alias="NUM_OF_CLASSES")

    kakao_allowed_origins: List[str] = Field(default_factory=list, alias="HDMeal_AllowedOrigins")
    auth_tokens: List[str] = Field(default_factory=list, alias="HDMeal_AuthTokens")

    base_user_settings_url: str = Field(..., alias="HDMeal_BaseURL")

    seoul_data_token: str = Field(..., alias="HDMeal_SeoulData_Token")
    kma_zone: str = Field(..., alias="HDMeal_KMAZone")

    jwt_secret: str = Field(..., alias="HDMeal_JWTSecret")
    cache_health_timetable_ttl_hours: int = 3
    cache_health_weather_ttl_hours: int = 1
    cache_health_water_temp_ttl_minutes: int = 76
    max_days_range: int = Field(default=31, alias="HDMeal_MaxDaysRange")

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
    )

    @field_validator("kakao_allowed_origins", mode="before")
    @classmethod
    def _split_origins(cls, value):
        if not value:
            return []
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                import json

                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def allowed_origins(self) -> List[str]:
        origins: List[str] = []
        for item in self.kakao_allowed_origins:
            candidate = item.strip()
            if not candidate:
                continue
            if candidate == "*":
                return ["*"]
            if candidate not in origins:
                origins.append(candidate)

        if self.base_user_settings_url:
            parts = urlsplit(self.base_user_settings_url)
            if parts.scheme and parts.netloc:
                origin = f"{parts.scheme}://{parts.netloc}"
                if origin not in origins:
                    origins.append(origin)

        for dev_origin in ("http://localhost:5173", "http://127.0.0.1:5173"):
            if dev_origin not in origins:
                origins.append(dev_origin)

        return origins or ["*"]

    @field_validator("auth_tokens", mode="before")
    @classmethod
    def _parse_auth_tokens(cls, value):
        if not value:
            return []
        if isinstance(value, str):
            import json

            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return [value]
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
            return [str(parsed)]
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
