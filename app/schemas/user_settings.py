from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, Field, model_validator


class UserSettingsResponse(BaseModel):
    classes: list[int]
    grades: list[int]
    current_grade: Optional[int] = None
    current_class: Optional[int] = None
    preferences: Dict[str, str] = Field(default_factory=dict)


class UpdateUserSettingsRequest(BaseModel):
    user_grade: int
    user_class: int
    preferences: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _convert_grade_class(cls, values):
        if isinstance(values, dict):
            values["user_grade"] = cls._to_int(values.get("user_grade"), "user_grade")
            values["user_class"] = cls._to_int(values.get("user_class"), "user_class")
        return values

    @staticmethod
    def _to_int(value, field_name: str) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            digits = "".join(ch for ch in value if ch.isdigit())
            if digits:
                return int(digits)
        raise ValueError(f"{field_name} 값이 올바르지 않습니다.")


class DeleteUserSettingsRequest(BaseModel):
    pass
