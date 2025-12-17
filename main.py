import os
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="Trading Desk Market Snapshot API", version="1.3.0")

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


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _ms_to_iso(ms: Optional[str]) -> Optional[str]:
    """
    OKX often returns timestamps in milliseconds as strings under field `ts`.
    Convert to ISO-8601 Z.
    """
    if not ms:
        return None
    try:
        ms_int = int(ms)
        dt = datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _vol_regime_from_change(pct_change_24h: Optional[float]) -> Optional[str]:
    if pct_change_24h is None:
        return None
    a = abs(pct_change_24h)
    if a < 2:
        return "low"
    if a < 5:
        return "medium"
    return "high"


def _liquidity_condition_from_quote_vol(vol_quote_24h: Optional[float]) -> Optional[str]:
    """
    Heurística simple usando volumen notional (quote). No es order book depth.
    """
    if vol_quote_24h is None:
        return None
    if vol_quote_24h <= 0:
        return "thin"
    if vol_quote_24h < 50_000_000:
        return "thin"
    if vol_quote_24h < 300_000_000:
        return "normal"
    return "crowded"


def _normalize_symbol_to_okx_inst(symbol: str) -> str:
    """
    GPT envía BTCUSDT / ETHUSDT.
    Mapeamos a perpetual swap OKX: BTC-USDT-SWAP
    """
    s = symbol.strip().upper()
    if not s.isalnum():
        raise ValueError("Invalid symbol format.")
    if not s.endswith("USDT"):
        raise ValueError("Only *USDT symbols supported (e.g., BTCUSDT).")
    base = s[:-4]
    return f"{base}-USDT-SWAP"


async def _okx_get(client: httpx.AsyncClient, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{OKX_BASE}{path}"
    r = await client.get(url, params=params)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OKX upstream error: {r.text}")
    data = r.json()
    # OKX normalmente retorna code == "0" en éxito
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

    cache_key = f"{exchange}:{inst_id}:{timeframe or ''}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=12.0) as client:
        # 1) Ticker (last, open24h, quote volume, ts)
        tick = await _okx_get(client, "/api/v5/market/ticker", {"instId": inst_id})
        tdata = tick.get("data", [])
        if not tdata:
            raise HTTPException(status_code=502, detail=f"No ticker data for {inst_id}")
        t0 = tdata[0]

        # OKX ticker fields are strings
        price = float(t0["last"]) if t0.get("last") is not None else None
        open_24h = float(t0["open24h"]) if t0.get("open24h") else None

        price_change_24h = None
        if price is not None and open_24h not in (None, 0.0):
            price_change_24h = ((price - open_24h) / open_24h) * 100.0

        # Prefer quote volume / notional fields if present
        # Depending on market, OKX may provide: volCcyQuote, volCcy24h, volCcy
        vol_quote_str = t0.get("volCcyQuote") or t0.get("volCcy24h") or t0.get("volCcy")
        volume_24h = float(vol_quote_str) if vol_quote_str else None

        timestamp_exchange = _ms_to_iso(t0.get("ts"))

        # 2) Funding rate
        fr = await _okx_get(client, "/api/v5/public/funding-rate", {"instId": inst_id})
        frd = fr.get("data", [])
        funding_rate = float(frd[0].get("fundingRate")) if frd and frd[0].get("fundingRate") is not None else None

        # 3) Open interest
        oi = await _okx_get(client, "/api/v5/public/open-interest", {"instType": "SWAP", "instId": inst_id})
        oid = oi.get("data", [])
        open_interest = None
        open_interest_unit = None
        if oid:
            # Prefer oiUsd if present; else fall back to oi (contracts)
            if oid[0].get("oiUsd") is not None:
                open_interest = float(oid[0]["oiUsd"])
                open_interest_unit = "USD"
            elif oid[0].get("oi") is not None:
                open_interest = float(oid[0]["oi"])
                open_interest_unit = "contracts"

    volatility_regime = _vol_regime_from_change(price_change_24h)
    liquidity_condition = _liquidity_condition_from_quote_vol(volume_24h)

    result = {
        # identity
        "symbol": symbol.strip().upper(),
        "exchange": exchange,
        "source": "okx",
        "venue": "okx",
        "inst_id": inst_id,

        # pricing
        "price": price,
        "price_quote": "USDT",
        "price_change_24h": price_change_24h,

        # volume (explicit unit)
        "volume_24h": volume_24h,
        "volume_24h_unit": "quote_notional",

        # derivatives
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "open_interest_unit": open_interest_unit,

        # optional placeholders for future extensions
        "oi_change": None,
        "long_short_ratio": None,

        # qualitative desk labels
        "volatility_regime": volatility_regime,
        "liquidity_condition": liquidity_condition,

        # timestamps
        "timestamp_exchange": timestamp_exchange,
        "timestamp": _iso_now(),
    }

    _cache_set(cache_key, result)
    return result

