import ccxt, asyncio, pandas as pd
from typing import List, Optional

def supported_timeframes():
    return {"1m","5m","15m","30m","1h","4h","1d"}

def parse_lookback(s: str) -> Optional[int]:
    '''
    '12h' -> 12 * 3600 * 1000
    '7d'  -> 7 * 24 * 3600 * 1000
    '12w' -> 12 * 7 * 24 * 3600 * 1000
    '24m' -> 24 * 30 * 24 * 3600 * 1000 (прибл.)
    '''
    try:
        s = s.strip().lower()
        if s.endswith("h"): return int(s[:-1]) * 3600 * 1000
        if s.endswith("d"): return int(s[:-1]) * 24 * 3600 * 1000
        if s.endswith("w"): return int(s[:-1]) * 7 * 24 * 3600 * 1000
        if s.endswith("m"): return int(s[:-1]) * 30 * 24 * 3600 * 1000
        return None
    except Exception:
        return None

async def fetch_ohlcv_multi(symbol: str, timeframe: str, lookback_ms: int, spot_exchanges: List[str], futures_exchanges: List[str]) -> pd.DataFrame:
    '''
    Возвращает DataFrame с индексом Datetime и колонками:
    close_spot, volume_spot, close_fut, volume_fut
    '''
    tasks = []
    now = int(pd.Timestamp.utcnow().value // 10**6)
    since = now - lookback_ms
    for ex in spot_exchanges:
        tasks.append(_fetch_one(ex, symbol, timeframe, since, "spot"))
    for ex in futures_exchanges:
        tasks.append(_fetch_one(ex, symbol, timeframe, since, "futures"))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    dfs_spot, dfs_fut = [], []
    for r in results:
        if isinstance(r, Exception) or r is None: continue
        df, mtype = r
        if df is None or df.empty: continue
        if mtype=="spot": dfs_spot.append(df[["close","volume"]].rename(columns={"close":"close_spot","volume":"volume_spot"}))
        else:            dfs_fut.append(df[["close","volume"]].rename(columns={"close":"close_fut","volume":"volume_fut"}))
    out = None
    if dfs_spot: out = _aggregate_same_market(dfs_spot, "close_spot", "volume_spot")
    if dfs_fut:
        fut = _aggregate_same_market(dfs_fut, "close_fut", "volume_fut")
        out = fut if out is None else out.join(fut, how="outer")
    if out is None: return pd.DataFrame()
    for c in ["close_spot","close_fut"]:
        if c in out: out[c] = out[c].ffill()
    for c in ["volume_spot","volume_fut"]:
        if c in out: out[c] = out[c].fillna(0)
    out = out.sort_index()
    start_ts = pd.to_datetime(since, unit="ms", utc=True)
    out = out[out.index >= start_ts]
    return out

async def _fetch_one(exchange_id: str, symbol: str, timeframe: str, since: int, market_type: str):
    try:
        ex_class = getattr(ccxt, exchange_id)
    except AttributeError:
        return None
    ex = ex_class({"enableRateLimit": True, "options": {"defaultType": "future" if market_type=="futures" else "spot"}})
    try:
        await _maybe_async(ex.load_markets)
    except Exception:
        pass
    limit = 1500
    try:
        ohlcv = await _maybe_async(ex.fetch_ohlcv, symbol, timeframe=timeframe, since=since, limit=limit)
    except Exception:
        return None
    if not ohlcv: return None
    df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df, market_type

async def _maybe_async(func, *args, **kwargs):
    import asyncio
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

def _aggregate_same_market(dfs: list, price_col: str, vol_col: str) -> pd.DataFrame:
    joined = None
    for i, df in enumerate(dfs):
        df = df.copy(); df.columns = [f"{c}_{i}" for c in df.columns]
        joined = df if joined is None else joined.join(df, how="outer")
    price_cols = [c for c in (joined.columns if joined is not None else []) if c.startswith("close")]
    vol_cols = [c for c in (joined.columns if joined is not None else []) if c.startswith("volume")]
    out = pd.DataFrame(index=joined.index)
    out[price_col] = joined[price_cols].median(axis=1, skipna=True)
    out[vol_col]  = joined[vol_cols].sum(axis=1, skipna=True)
    return out
