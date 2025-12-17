import os
import time
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="Trading Desk Market Snapshot API", version="1.0.0")

BINANCE_SPOT_BASE = os.getenv("BINANCE_SPOT_BASE", "https://api.binance.com")
BINANCE_FUTURES_BASE = os.getenv("BINANCE_FUTURES_BASE", "https://fapi.binance.com")

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

def _liquidity_condition_from_volume(volume_24h_quote: float) -> str:
    if volume_24h_quote <= 0:
        return "thin"
    if volume_24h_quote < 50_000_000:
        return "thin"
    if volume_24h_quote < 300_000_000:
        return "normal"
    return "crowded"

@app.get("/")
def health():
    return {"status": "ok", "service": "crypto-market-snapshot"}

@app.get("/market_snapshot")
async def market_snapshot(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT, ETHUSDT"),
    exchange: str = Query("binance", description="Venue label"),
    timeframe: Optional[str] = Query(None, description="Optional timeframe label, e.g. 4H, 1D"),
):
    if not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid symbol format.")

    key = f"{exchange}:{symbol}:{timeframe or ''}"
    cached = _cache_get(key)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Spot 24h ticker
        spot_resp = await client.get(
            f"{BINANCE_SPOT_BASE}/api/v3/ticker/24hr",
            params={"symbol": symbol},
        )
        if spot_resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Spot upstream error: {spot_resp.text}")
        spot = spot_resp.json()

        try:
            price = float(spot["lastPrice"])
            volume_24h_quote = float(spot["quoteVolume"])
            price_change_24h = float(spot["priceChangePercent"])
        except Exception:
            raise HTTPException(status_code=502, detail="Unexpected spot payload format.")

        # Futures Open Interest (may fail if symbol not listed on futures)
        open_interest = None
        oi_resp = await client.get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest",
            params={"symbol": symbol},
        )
        if oi_resp.status_code == 200:
            oi = oi_resp.json()
            try:
                open_interest = float(oi.get("openInterest"))
            except Exception:
                open_interest = None

        # Futures Funding Rate (latest)
        funding_rate = None
        fr_resp = await client.get(
            f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
        )
        if fr_resp.status_code == 200:
            fr = fr_resp.json()
            if isinstance(fr, list) and fr:
                try:
                    funding_rate = float(fr[0].get("fundingRate"))
                except Exception:
                    funding_rate = None

    volatility_regime = _vol_regime_from_change(price_change_24h)
    liquidity_condition = _liquidity_condition_from_volume(volume_24h_quote)

    result = {
        "symbol": symbol,
        "exchange": exchange,
        "price": price,
        "price_change_24h": price_change_24h,
        "volume_24h": volume_24h_quote,
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "oi_change": None,
        "long_short_ratio": None,
        "volatility_regime": volatility_regime,
        "liquidity_condition": liquidity_condition,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "binance"
    }

    _cache_set(key, result)
    return result
