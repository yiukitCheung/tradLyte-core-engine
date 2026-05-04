"""FastAPI application for TradLyte serving endpoints."""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from serving_api.routers.backtest import router as backtest_router
from serving_api.routers.market import router as market_router
from serving_api.routers.picks import router as picks_router
from serving_api.routers.screener import router as screener_router

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = FastAPI(title="TradLyte Serving API", version="0.1.0")

allowed_origin = os.environ.get("ALLOWED_ORIGIN", "*")
allow_origins = [allowed_origin] if allowed_origin != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["content-type", "x-api-key"],
)

def require_api_key(x_api_key: Optional[str] = Header(default=None, alias="x-api-key")) -> None:
    expected_key = os.environ.get("SERVING_API_KEY")
    if not expected_key:
        return
    if not x_api_key or x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": "http_error", "message": str(exc.detail)}},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled serving API error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "internal_error", "message": "Internal server error"}},
    )


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "dev-serving-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


app.include_router(screener_router, dependencies=[Depends(require_api_key)])
app.include_router(picks_router, dependencies=[Depends(require_api_key)])
app.include_router(market_router, dependencies=[Depends(require_api_key)])
app.include_router(backtest_router, dependencies=[Depends(require_api_key)])
