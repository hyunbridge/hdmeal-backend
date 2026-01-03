from __future__ import annotations

import time
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import get_settings
from .db import close_client, get_database
from .exceptions import HDMealException
from .routers import app_api, chatbot
from .services.data_service import DataService
from .services.ingestion_service import IngestionService
from .tasks.scheduler import PeriodicTask
from .utils.logging import get_logger, setup_logging
from .utils import security

# Configure logging before anything else
setup_logging()

settings = get_settings()
logger = get_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown tasks."""
    # Startup: Prime database indexes and warm up a 10-day data window.
    database = get_database()
    data_service = DataService(database)
    await data_service.ensure_indexes()
    ingestion_service = IngestionService(data_service)
    try:
        await ingestion_service.sync_window()
    except Exception:
        logger.exception("Startup sync failed; continuing without warm cache")

    refresh_task = PeriodicTask(
        interval=timedelta(hours=3),
        coro_factory=lambda: ingestion_service.sync_window(),
    )
    refresh_task.start()
    logger.info("Application startup complete")

    yield

    # Shutdown: Stop periodic tasks
    refresh_task.stop()
    close_client()
    logger.info("Application shutdown complete")


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.exception_handler(HDMealException)
async def hdmeal_exception_handler(request: Request, exc: HDMealException):
    """Handle custom HDMeal exceptions."""
    req_id = getattr(request.state, "req_id", request.headers.get("X-HDMeal-Req-ID", "unknown"))
    logger.error(f"[{req_id}] HDMeal exception: {exc.message}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message, "requestId": req_id},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    req_id = getattr(request.state, "req_id", request.headers.get("X-HDMeal-Req-ID", "unknown"))
    logger.exception(f"[{req_id}] Unexpected error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "서버 오류가 발생했습니다", "requestId": req_id},
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests with timing information."""
    start_time = time.time()
    req_id = request.headers.get("X-HDMeal-Req-ID") or security.generate_req_id()
    request.state.req_id = req_id

    logger.info(f"[{req_id}] {request.method} {request.url.path}")

    response = await call_next(request)

    process_time = (time.time() - start_time) * 1000
    logger.info(
        f"[{req_id}] {request.method} {request.url.path} "
        f"- Status: {response.status_code} - {process_time:.2f}ms"
    )

    response.headers.setdefault("X-HDMeal-Req-ID", req_id)
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials="*" not in settings.allowed_origins,
)

app.include_router(chatbot.router)
app.include_router(app_api.router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
