"""Logging configuration for HDMeal backend."""
from __future__ import annotations

import logging
import sys
from typing import Any

from ..config import get_settings

settings = get_settings()


def setup_logging() -> None:
    """Configure application-wide logging."""
    log_level = logging.DEBUG if settings.debug else logging.INFO

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set specific logger levels
    logging.getLogger("hdmeal").setLevel(log_level)
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module."""
    return logging.getLogger(f"hdmeal.{name}")
