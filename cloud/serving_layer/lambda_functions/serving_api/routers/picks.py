"""Stock pick API routes."""

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, HTTPException, Query

from serving_api.cache import PICKS_CACHE, RETURNS_CACHE, make_cache_key
from serving_api.db import execute_query

router = APIRouter(prefix="/picks", tags=["picks"])


def _to_return(pick_price: Optional[Decimal], close_price: Optional[Decimal]) -> Optional[float]:
    if pick_price is None or close_price is None:
        return None
    if float(pick_price) == 0.0:
        return None
    return float((close_price - pick_price) / pick_price)


def _picks_today_meta_filters(
    industry: Optional[str],
    min_market_cap: Optional[int],
    max_market_cap: Optional[int],
) -> Dict[str, Any]:
    """Params + WHERE clause fragment for optional symbol_metadata filters (matches screener semantics)."""
    ind = industry.strip() if industry else None
    if ind == "":
        ind = None
    return {
        "industry": ind,
        "min_mc": min_market_cap,
        "max_mc": max_market_cap,
    }


_PICKS_TODAY_JOIN_WHERE = """
        FROM stock_picks sp
        LEFT JOIN symbol_metadata m ON m.symbol = sp.symbol
        WHERE sp.scan_date = (SELECT d FROM latest)
          AND (%(industry)s::text IS NULL OR m.industry = %(industry)s)
          AND (%(min_mc)s::bigint IS NULL OR m.marketcap >= %(min_mc)s)
          AND (%(max_mc)s::bigint IS NULL OR m.marketcap <= %(max_mc)s)
        ORDER BY m.marketcap DESC
"""


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


@router.get("/today")
def get_picks_today(
    limit: int = Query(default=25, ge=1, le=200),
    industry: Optional[str] = Query(default=None, description="Exact match on symbol_metadata.industry"),
    min_market_cap: Optional[int] = Query(default=None, ge=0, description="Minimum marketcap (symbol_metadata)"),
    max_market_cap: Optional[int] = Query(default=None, ge=0, description="Maximum marketcap (symbol_metadata)"),
) -> Dict[str, Any]:
    filt = _picks_today_meta_filters(industry, min_market_cap, max_market_cap)
    cache_key = make_cache_key(
        "picks_today",
        {"limit": limit, **filt},
    )
    cached = PICKS_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    params = {"limit": limit, **filt}
    query = f"""
        WITH latest AS (SELECT MAX(scan_date) AS d FROM stock_picks)
        SELECT sp.scan_date,
               sp.rank,
               sp.symbol,
               sp.strategy_name,
               sp.signal,
               sp.price,
               sp.confidence,
               sp.metadata
        {_PICKS_TODAY_JOIN_WHERE}
        ORDER BY sp.rank ASC
        LIMIT %(limit)s;
    """
    rows = execute_query(query, params=params)
    scan_date = rows[0]["scan_date"] if rows else None
    response = {
        "data": rows,
        "meta": {
            "count": len(rows),
            "limit": limit,
            "scan_date": scan_date,
            "filters": {
                "industry": filt["industry"],
                "min_market_cap": filt["min_mc"],
                "max_market_cap": filt["max_mc"],
            },
            "cache_hit": False,
        },
    }
    PICKS_CACHE[cache_key] = response
    return response


@router.get("/today/metadata")
def get_picks_today_metadata(
    limit: int = Query(default=25, ge=1, le=200),
    industry: Optional[str] = Query(default=None, description="Exact match on symbol_metadata.industry"),
    min_market_cap: Optional[int] = Query(default=None, ge=0, description="Minimum marketcap (symbol_metadata)"),
    max_market_cap: Optional[int] = Query(default=None, ge=0, description="Maximum marketcap (symbol_metadata)"),
) -> Dict[str, Any]:
    filt = _picks_today_meta_filters(industry, min_market_cap, max_market_cap)
    cache_key = make_cache_key(
        "picks_today_metadata",
        {"limit": limit, **filt},
    )
    cached = PICKS_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    params = {"limit": limit, **filt}
    query = f"""
        WITH latest AS (SELECT MAX(scan_date) AS d FROM stock_picks)
        SELECT sp.scan_date,
               sp.rank,
               sp.symbol,
               sp.strategy_name,
               sp.metadata
        {_PICKS_TODAY_JOIN_WHERE}
        ORDER BY sp.rank ASC
        LIMIT %(limit)s;
    """
    rows = execute_query(query, params=params)
    scan_date = rows[0]["scan_date"] if rows else None
    response = {
        "data": rows,
        "meta": {
            "count": len(rows),
            "limit": limit,
            "scan_date": scan_date,
            "filters": {
                "industry": filt["industry"],
                "min_market_cap": filt["min_mc"],
                "max_market_cap": filt["max_mc"],
            },
            "cache_hit": False,
        },
    }
    PICKS_CACHE[cache_key] = response
    return response


@router.get("/detail")
def get_pick_detail(
    symbol: str = Query(..., min_length=1, max_length=50, description="Ticker, e.g. AAPL"),
    scan_date: date = Query(..., description="Market scan date (YYYY-MM-DD)"),
    strategy_name: Optional[str] = Query(
        default=None,
        max_length=255,
        description="If set, return at most one row for this strategy",
    ),
) -> Dict[str, Any]:
    """
    Pick row(s) from `stock_picks` for a symbol + scan date, joined with
    `symbol_metadata` for industry, market cap, and basic listing fields.
    """
    sym = symbol.strip().upper()
    cache_key = make_cache_key(
        "pick_detail",
        {
            "symbol": sym,
            "scan_date": scan_date.isoformat(),
            "strategy_name": strategy_name or "",
        },
    )
    cached = PICKS_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    params: Dict[str, Any] = {
        "symbol": sym,
        "scan_date": scan_date.isoformat(),
    }
    strategy_clause = ""
    if strategy_name:
        params["strategy_name"] = strategy_name.strip()
        strategy_clause = "AND sp.strategy_name = %(strategy_name)s"

    query = f"""
        SELECT sp.scan_date,
               sp.rank,
               sp.symbol,
               sp.strategy_name,
               sp.signal,
               sp.price,
               sp.confidence,
               sp.metadata AS pick_metadata,
               m.name          AS asset_name,
               m.market,
               m.locale,
               m.type          AS asset_type,
               m.primary_exchange,
               m.industry,
               m.marketcap   AS market_cap
        FROM stock_picks sp
        LEFT JOIN symbol_metadata m ON m.symbol = sp.symbol
        WHERE sp.scan_date = %(scan_date)s::date
          AND UPPER(TRIM(sp.symbol)) = %(symbol)s
          {strategy_clause}
        ORDER BY sp.rank ASC, sp.strategy_name ASC;
    """
    rows = execute_query(query, params=params)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No stock_picks row for symbol={sym} scan_date={scan_date.isoformat()}"
            + (f" strategy_name={strategy_name}" if strategy_name else ""),
        )

    response = {
        "data": rows,
        "meta": {
            "count": len(rows),
            "symbol": sym,
            "scan_date": scan_date.isoformat(),
            "strategy_filter": strategy_name,
            "cache_hit": False,
        },
    }
    PICKS_CACHE[cache_key] = response
    return response


@router.get("/{scan_date}/returns")
def get_pick_returns(
    scan_date: date,
    horizons: str = Query(default="1,5,21"),
    industry: Optional[str] = Query(default=None, description="Exact match on symbol_metadata.industry"),
    min_market_cap: Optional[int] = Query(default=None, ge=0, description="Minimum marketcap (symbol_metadata)"),
    max_market_cap: Optional[int] = Query(default=None, ge=0, description="Maximum marketcap (symbol_metadata)"),
) -> Dict[str, Any]:
    filt = _picks_today_meta_filters(industry, min_market_cap, max_market_cap)
    selected_horizons = _parse_horizons(horizons)
    cache_key = make_cache_key(
        "pick_returns",
        {
            "scan_date": scan_date.isoformat(),
            "horizons": ",".join(map(str, selected_horizons)),
            **filt,
        },
    )
    cached = RETURNS_CACHE.get(cache_key)
    if cached is not None:
        cached["meta"]["cache_hit"] = True
        return cached

    params: Dict[str, Any] = {
        "scan_date": scan_date.isoformat(),
        "industry": filt["industry"],
        "min_mc": filt["min_mc"],
        "max_mc": filt["max_mc"],
    }
    query = """
        WITH picks AS (
            SELECT sp.symbol,
                   sp.rank,
                   sp.strategy_name,
                   sp.signal,
                   sp.price AS pick_price,
                   sp.scan_date
            FROM stock_picks sp
            LEFT JOIN symbol_metadata m ON m.symbol = sp.symbol
            WHERE sp.scan_date = %(scan_date)s::date
              AND (%(industry)s::text IS NULL OR m.industry = %(industry)s)
              AND (%(min_mc)s::bigint IS NULL OR m.marketcap >= %(min_mc)s)
              AND (%(max_mc)s::bigint IS NULL OR m.marketcap <= %(max_mc)s)
        )
        SELECT p.*,
               (
                    SELECT close
                    FROM raw_ohlcv
                    WHERE symbol = p.symbol
                      AND interval = '1d'
                      AND timestamp::date > p.scan_date
                    ORDER BY timestamp ASC
                    LIMIT 1 OFFSET 0
               ) AS close_1d,
               (
                    SELECT close
                    FROM raw_ohlcv
                    WHERE symbol = p.symbol
                      AND interval = '1d'
                      AND timestamp::date > p.scan_date
                    ORDER BY timestamp ASC
                    LIMIT 1 OFFSET 4
               ) AS close_5d,
               (
                    SELECT close
                    FROM raw_ohlcv
                    WHERE symbol = p.symbol
                      AND interval = '1d'
                      AND timestamp::date > p.scan_date
                    ORDER BY timestamp ASC
                    LIMIT 1 OFFSET 20
               ) AS close_21d,
               (
                    SELECT close
                    FROM raw_ohlcv
                    WHERE symbol = p.symbol
                      AND interval = '1d'
                    ORDER BY timestamp DESC
                    LIMIT 1
               ) AS close_now
        FROM picks p
        ORDER BY p.rank;
    """
    rows = execute_query(query, params=params)

    data = []
    for row in rows:
        return_map: Dict[str, Optional[float]] = {}
        close_map = {
            1: row.get("close_1d"),
            5: row.get("close_5d"),
            21: row.get("close_21d"),
        }
        for horizon in selected_horizons:
            close_value = close_map.get(horizon)
            return_map[f"{horizon}d"] = _to_return(row.get("pick_price"), close_value)
        data.append(
            {
                "scan_date": row["scan_date"],
                "rank": row["rank"],
                "symbol": row["symbol"],
                "strategy_name": row["strategy_name"],
                "signal": row["signal"],
                "pick_price": row["pick_price"],
                "close_now": row.get("close_now"),
                "return_to_date": _to_return(row.get("pick_price"), row.get("close_now")),
                "returns": return_map,
            }
        )

    response = {
        "data": data,
        "meta": {
            "count": len(data),
            "scan_date": scan_date.isoformat(),
            "horizons": selected_horizons,
            "filters": {
                "industry": filt["industry"],
                "min_market_cap": filt["min_mc"],
                "max_market_cap": filt["max_mc"],
            },
            "cache_hit": False,
        },
    }
    RETURNS_CACHE[cache_key] = response
    return response
