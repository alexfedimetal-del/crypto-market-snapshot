import os
import time
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="Trading Desk Market Snapshot API", version="1.2.0")

OKX_BASE = os.getenv("OKX_BASE", "https://www.okx.com")
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

def _liquidity_condition_from_quote_vol(vol_quote_24h: float) -> str:
    if vol_quote_24h <= 0:
        return "thin"
    if vol_quote_24h < 50_000_000:
        return "thin"
    if vol_quote_24h < 300_000_000:
        return "normal"
    return "crowded"

def _normalize_symbol_to_okx_inst(symbol: str) -> str:
    """
    GPT sends BTCUSDT / ETHUSDT style.
    We map to OKX perpetual swap: BTC-USDT-SWAP
    """
    s = symbol.strip().upper()
    if not s.isalnum():
        raise ValueError("Invalid symbol format.")
    if not s.endswith("USDT"):
        raise ValueError("Only *USDT symbols supported in this minimal version (e.g., BTCUSDT).")
    base = s[:-4]
    return f"{base}-USDT-SWAP"

async def _okx_get(client: httpx.AsyncClient, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{OKX_BASE}{path}"
    r = await client.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OKX upstream error: {r.text}")
    data = r.json()
    # OKX v5 typically returns code == "0" on success
    if isinstance(data, dict) and data.get("code") not in ("0", 0, None):
        raise HTTPException(status_code=502, detail=f"OKX code error: {data}")
    return data

@app.get("/")
def health():
    return {"status": "ok", "service": "crypto-market-snapshot", "source": "okx"}

@app.get("/market_snapshot")
async def market_snapshot(
    symbol: str = Query(..., description="e.g. BTCUSDT, ETHUSDT"),
    exchange: str = Query("okx", description="Venue label"),
    timeframe: Optional[str] = Query(None, description="Optional timeframe label, e.g. 4H, 1D"),
):
    try:
        inst_id = _normalize_symbol_to_okx_inst(symbol)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    key = f"{exchange}:{inst_id}:{timeframe or ''}"
    cached = _cache_get(key)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=12.0) as client:
        # Ticker: GET /api/v5/market/ticker?instId=BTC-USDT-SWAP :contentReference[oaicite:1]{index=1}
        tick = await _okx_get(client, "/api/v5/market/ticker", {"instId": inst_id})
        data = tick.get("data", [])
        if not data:
            raise HTTPException(status_code=502, detail=f"No ticker data for {inst_id}")
        t0 = data[0]

        # Common OKX ticker fields include last, open24h, volCcyQuote (quote vol)
        price = float(t0.get("last"))
        open_24h = float(t0.get("open24h")) if t0.get("open24h") else None

        # If open24h is missing, we’ll compute change as None
        price_change_24h = None
        if open_24h and open_24h != 0:
            price_change_24h = ((price - open_24h) / open_24h) * 100.0

        # Quote volume: OKX exposes quote volume fields (e.g., volCcyQuote) in some endpoints :contentReference[oaicite:2]{index=2}
        vol_quote = t0.get("volCcyQuote") or t0.get("volCcy24h") or t0.get("volCcy")
        volume_24h = float(vol_quote) if vol_quote else None

        # Funding: GET /api/v5/public/funding-rate?instId=BTC-USDT-SWAP :contentReference[oaicite:3]{index=3}
        fr = await _okx_get(client, "/api/v5/public/funding-rate", {"instId": inst_id})
        frd = fr.get("data", [])
        funding_rate = float(frd[0].get("fundingRate")) if frd else None

        # Open interest: OKX has a “Get open interest” endpoint (changelog references it and oiUsd field) :contentReference[oaicite:4]{index=4}
        # Typical params are instType + instId; we use SWAP + instId.
        oi = await _okx_get(client, "/api/v5/public/open-interest", {"instType": "SWAP", "instId": inst_id})
        oid = oi.get("data", [])
        open_interest = None
        if oid:
            # Prefer oiUsd if present; otherwise oi
            open_interest = oid[0].get("oiUsd") or oid[0].get("oi")
            open_interest = float(open_interest) if open_interest is not None else None

    # Qualitative labels (only if we have change/volume)
    volatility_regime = _vol_regime_from_change(price_change_24h) if price_change_24h is not None else None
    liquidity_condition = _liquidity_condition_from_quote_vol(volume_24h) if volume_24h is not None else None

    result = {
        "symbol": symbol.strip().upper(),
        "exchange": exchange,
        "price": price,
        "price_change_24h": price_change_24h,
        "volume_24h": volume_24h,
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "oi_change": None,
        "long_short_ratio": None,
        "volatility_regime": volatility_regime,
        "liquidity_condition": liquidity_condition,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "okx",
        "instId": inst_id,
    }

    _cache_set(key, result)
    return result


