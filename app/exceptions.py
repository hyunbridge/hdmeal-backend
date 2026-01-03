"""Custom exceptions for HDMeal backend."""
from __future__ import annotations


class HDMealException(Exception):
    """Base exception for all HDMeal errors."""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class ExternalAPIError(HDMealException):
    """Exception raised when external API calls fail."""

    def __init__(self, service: str, message: str = "외부 API 연결에 실패했습니다"):
        self.service = service
        super().__init__(f"{message}: {service}", status_code=503)


class DataNotFoundError(HDMealException):
    """Exception raised when requested data is not found."""

    def __init__(self, resource: str, message: str = "요청한 데이터를 찾을 수 없습니다"):
        self.resource = resource
        super().__init__(f"{message}: {resource}", status_code=404)


class ValidationError(HDMealException):
    """Exception raised when data validation fails."""

    def __init__(self, field: str, message: str = "입력값이 올바르지 않습니다"):
        self.field = field
        super().__init__(f"{message}: {field}", status_code=400)


class AuthenticationError(HDMealException):
    """Exception raised when authentication fails."""

    def __init__(self, message: str = "인증에 실패했습니다"):
        super().__init__(message, status_code=401)


class AuthorizationError(HDMealException):
    """Exception raised when authorization fails."""

    def __init__(self, message: str = "권한이 없습니다"):
        super().__init__(message, status_code=403)
