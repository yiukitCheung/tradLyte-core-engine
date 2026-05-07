"""Market data API routes (quote, OHLCV, returns, Polygon news)."""

import json
import os
from functools import lru_cache
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import boto3
from fastapi import APIRouter, HTTPException, Query

from serving_api.cache import MARKET_CACHE, make_cache_key
from serving_api.db import execute_one, execute_query

router = APIRouter(prefix="/market", tags=["market"])

SORT_DIRECTIONS = {"asc", "desc"}
INTERVALS = {"1d", "1h", "15m", "5m", "1m"}
secrets_client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "ca-west-1"))


def _to_return(base_price: Optional[Decimal], latest_price: Optional[Decimal]) -> Optional[float]:
    if base_price is None or latest_price is None:
        return None
    if float(base_price) == 0.0:
        return None
    return float((latest_price - base_price) / base_price)


def _parse_horizons(raw_horizons: str) -> List[int]:
    horizons: Set[int] = set()
    for token in raw_horizons.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            continue
        if value <= 0 or value > 252:
            continue
        horizons.add(value)
    if not horizons:
        horizons = {1, 5, 21}
    return sorted(horizons)


def _strip_api_key_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return url
    parts = urlsplit(url)
    params = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "apikey"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))


@lru_cache(maxsize=1)
def _get_polygon_api_key() -> str:
    secret_arn = os.environ.get("POLYGON_API_KEY_SECRET_ARN", "").strip()
    if not secret_arn:
        raise RuntimeError("POLYGON_API_KEY_SECRET_ARN is not configured")

    secret = secrets_client.get_secret_value(SecretId=secret_arn)
    secret_string = secret.get("SecretString") or "{}"
    payload = json.loads(secret_string)
    api_key = (payload.get("POLYGON_API_KEY") or payload.get("apiKey") or "").strip()
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY not found in Secrets Manager payload")
    return api_key


def _fetch_polygon_news(
    symbol: str,
    limit: int,
    order: str,
    published_utc_gte: Optional[str],
    published_utc_lte: Optional[str],
) -> Dict[str, Any]:
    api_key = _get_polygon_api_key()
    params: Dict[str, Any] = {
        "ticker": symbol,
        "order": order,
        "sort": "published_utc",
        "limit": limit,
        "apiKey": api_key,
    }
    if published_utc_gte:
        params["published_utc.gte"] = published_utc_gte
    if published_utc_lte:
        params["published_utc.lte"] = published_utc_lte

    base_url = os.environ.get("POLYGON_NEWS_URL", "https://api.polygon.io/v2/reference/news")
    url = f"{base_url}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=12) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise HTTPException(
            status_code=502,
            detail=f"Polygon news request failed ({exc.code}): {detail[:400]}",
        ) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"Polygon news unreachable: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="Polygon news returned non-JSON response") from exc
    return data


@router.get("/quote/{symbol}")
def get_latest_quote(symbol: str) -> Dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    cache_key = make_cache_key("market_quote", {"symbol": normalized_symbol})
    cached = MARKET_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    row = execute_one(
        """
        SELECT m.symbol,
               m.name,
               m.industry,
               m.marketcap AS market_cap,
               m.type,
               m.primary_exchange,
               o.timestamp::date AS as_of_date,
               o.open,
               o.high,
               o.low,
               o.close,
               o.volume
        FROM symbol_metadata m
        JOIN LATERAL (
            SELECT timestamp, open, high, low, close, volume
            FROM raw_ohlcv
            WHERE symbol = m.symbol
              AND interval = '1d'
            ORDER BY timestamp DESC
            LIMIT 1
        ) o ON TRUE
        WHERE m.symbol = %(symbol)s
        LIMIT 1;
        """,
        params={"symbol": normalized_symbol},
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Symbol not found or no OHLCV data: {normalized_symbol}")

    response = {"data": row, "meta": {"symbol": normalized_symbol, "cache_hit": False}}
    MARKET_CACHE[cache_key] = response
    return response


@router.get("/ohlcv/{symbol}")
def get_ohlcv_history(
    symbol: str,
    interval: str = Query(default="1d"),
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    sort: str = Query(default="desc"),
) -> Dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    normalized_interval = interval.strip().lower()
    normalized_sort = sort.strip().lower()

    if normalized_interval not in INTERVALS:
        raise HTTPException(status_code=400, detail=f"Unsupported interval: {interval}")
    if normalized_sort not in SORT_DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported sort: {sort}")

    params: Dict[str, Any] = {
        "symbol": normalized_symbol,
        "interval": normalized_interval,
        "limit": limit,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
    }
    cache_key = make_cache_key("market_ohlcv", params | {"sort": normalized_sort})
    cached = MARKET_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    query = f"""
        SELECT symbol,
               interval,
               timestamp,
               timestamp::date AS trading_date,
               open,
               high,
               low,
               close,
               volume
        FROM raw_ohlcv
        WHERE symbol = %(symbol)s
          AND interval = %(interval)s
          AND (%(start_date)s::date IS NULL OR timestamp::date >= %(start_date)s::date)
          AND (%(end_date)s::date IS NULL OR timestamp::date <= %(end_date)s::date)
        ORDER BY timestamp {normalized_sort.upper()}
        LIMIT %(limit)s;
    """
    rows = execute_query(query, params=params)
    response = {
        "data": rows,
        "meta": {
            "symbol": normalized_symbol,
            "interval": normalized_interval,
            "count": len(rows),
            "limit": limit,
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "sort": normalized_sort,
            "cache_hit": False,
        },
    }
    MARKET_CACHE[cache_key] = response
    return response


@router.get("/news/{symbol}")
def get_symbol_news(
    symbol: str,
    limit: int = Query(default=10, ge=1, le=50),
    order: str = Query(default="desc"),
    published_utc_gte: Optional[date] = Query(default=None),
    published_utc_lte: Optional[date] = Query(default=None),
) -> Dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    normalized_order = order.strip().lower()
    if normalized_order not in SORT_DIRECTIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported order: {order}")
    if published_utc_gte and published_utc_lte and published_utc_gte > published_utc_lte:
        raise HTTPException(status_code=400, detail="published_utc_gte must be on or before published_utc_lte")

    cache_key = make_cache_key(
        "market_news",
        {
            "symbol": normalized_symbol,
            "limit": limit,
            "order": normalized_order,
            "published_utc_gte": published_utc_gte.isoformat() if published_utc_gte else None,
            "published_utc_lte": published_utc_lte.isoformat() if published_utc_lte else None,
        },
    )
    cached = MARKET_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    payload = _fetch_polygon_news(
        symbol=normalized_symbol,
        limit=limit,
        order=normalized_order,
        published_utc_gte=published_utc_gte.isoformat() if published_utc_gte else None,
        published_utc_lte=published_utc_lte.isoformat() if published_utc_lte else None,
    )
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    response = {
        "data": rows,
        "meta": {
            "symbol": normalized_symbol,
            "count": len(rows),
            "limit": limit,
            "order": normalized_order,
            "published_utc_gte": published_utc_gte.isoformat() if published_utc_gte else None,
            "published_utc_lte": published_utc_lte.isoformat() if published_utc_lte else None,
            "next_url": _strip_api_key_from_url(payload.get("next_url")) if isinstance(payload, dict) else None,
            "cache_hit": False,
        },
    }
    MARKET_CACHE[cache_key] = response
    return response


@router.get("/returns/{symbol}")
def get_symbol_returns(
    symbol: str,
    horizons: str = Query(default="1,5,21"),
) -> Dict[str, Any]:
    normalized_symbol = symbol.upper().strip()
    selected_horizons = _parse_horizons(horizons)
    cache_key = make_cache_key(
        "market_returns",
        {"symbol": normalized_symbol, "horizons": ",".join(map(str, selected_horizons))},
    )
    cached = MARKET_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    row = execute_one(
        """
        WITH prices AS (
            SELECT timestamp::date AS trade_date,
                   close,
                   ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
            FROM raw_ohlcv
            WHERE symbol = %(symbol)s
              AND interval = '1d'
        )
        SELECT MAX(CASE WHEN rn = 1 THEN trade_date END) AS as_of_date,
               MAX(CASE WHEN rn = 1 THEN close END) AS close_now,
               MAX(CASE WHEN rn = 2 THEN close END) AS close_1d_ago,
               MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago,
               MAX(CASE WHEN rn = 22 THEN close END) AS close_21d_ago
        FROM prices;
        """,
        params={"symbol": normalized_symbol},
    )
    if not row or row.get("close_now") is None:
        raise HTTPException(status_code=404, detail=f"No OHLCV data found for symbol: {normalized_symbol}")

    close_map = {1: row.get("close_1d_ago"), 5: row.get("close_5d_ago"), 21: row.get("close_21d_ago")}
    returns_map: Dict[str, Optional[float]] = {}
    for horizon in selected_horizons:
        returns_map[f"{horizon}d"] = _to_return(close_map.get(horizon), row.get("close_now"))

    response = {
        "data": {
            "symbol": normalized_symbol,
            "as_of_date": row.get("as_of_date"),
            "close_now": row.get("close_now"),
            "returns": returns_map,
        },
        "meta": {
            "horizons": selected_horizons,
            "cache_hit": False,
        },
    }
    MARKET_CACHE[cache_key] = response
    return response
