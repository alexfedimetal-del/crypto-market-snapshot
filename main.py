import os
import time
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="Trading Desk Market Snapshot API", version="1.1.0")

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "8"))
_cache: Dict[str, Dict[str, Any]] = {}

def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() - item["ts"] > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return item["value"]

def _cache_set(key: str, value: Dict[str, Any]) -> None:
    _cache[key] = {"ts": time.time(), "value": value}

def _vol_regime_from_change(pct_change_24h: float) -> str:
    a = abs(pct_change_24h)
    if a < 2:
        return "low"
    if a < 5:
        return "medium"
    return "high"

def _liquidity_condition_from_turnover(turnover_24h: float) -> str:
    if turnover_24h <= 0:
        return "thin"
    if turnover_24h < 50_000_000:
        return "thin"
    if turnover_24h < 300_000_000:
        return "normal"
    return "crowded"

def _normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if not s.isalnum():
        raise ValueError("Invalid symbol format")
    return s

async def _bybit_get(client: httpx.AsyncClient, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BYBIT_BASE}{path}"
    r = await client.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Bybit upstream error: {r.text}")
    data = r.json()
    if isinstance(data, dict) and data.get("retCode") not in (0, None):
        raise HTTPException(status_code=502, detail=f"Bybit retCode error: {data}")
    return data

@app.get("/")
def health():
    return {"status": "ok", "service": "crypto-market-snapshot"}

@app.get("/market_snapshot")
async def market_snapshot(
    symbol: str = Query(..., description="e.g. BTCUSDT, ETHUSDT"),
    exchange: str = Query("bybit"),
    timeframe: Optional[str] = Query(None),
):
    try:
        sym = _normalize_symbol(symbol)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    key = f"{exchange}:{sym}:{timeframe or ''}"
    cached = _cache_get(key)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=12.0) as client:
        ticker = await _bybit_get(
            client,
            "/v5/market/tickers",
            {"category": "linear", "symbol": sym},
        )
        lst = ticker.get("result", {}).get("list", [])
        if not lst:
            raise HTTPException(status_code=502, detail=f"No ticker data for {sym}")
        t0 = lst[0]

        price = float(t0["lastPrice"])
        price_change_24h = float(t0.get("price24hPcnt", 0.0)) * 100.0
        turnover_24h = float(t0.get("turnover24h", 0.0))

        oi = await _bybit_get(
            client,
            "/v5/market/open-interest",
            {"category": "linear", "symbol": sym, "intervalTime": "5min", "limit": 1},
        )
        oi_list = oi.get("result", {}).get("list", [])
        open_interest = float(oi_list[0]["openInterest"]) if oi_list else None

        fr = await _bybit_get(
            client,
            "/v5/market/funding/history",
            {"category": "linear", "symbol": sym, "limit": 1},
        )
        fr_list = fr.get("result", {}).get("list", [])
        funding_rate = float(fr_list[0]["fundingRate"]) if fr_list else None

    result = {
        "symbol": sym,
        "exchange": exchange,
        "price": price,
        "price_change_24h": price_change_24h,
        "volume_24h": turnover_24h,
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "volatility_regime": _vol_regime_from_change(price_change_24h),
        "liquidity_condition": _liquidity_condition_from_turnover(turnover_24h),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "bybit",
    }

    _cache_set(key, result)
    return result

