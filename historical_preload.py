# ================= HISTORICAL PRELOAD — EMA 9 WAVE =================
#
# Fetches up to 30 calendar days of OHLCV data from Fyers REST API
# for every symbol × every configured timeframe.
#
# Storage schema (MongoDB):
#   DB  : ema9_wave
#   COL : candles_{tf}   (one collection per timeframe, e.g. candles_3)
#   DOC : {
#           symbol   : "NSE:RELIANCE-EQ",
#           datetime : ISODate(...),          ← UTC stored, IST on read
#           open, high, low, close, volume : float/int
#         }
#   INDEX: unique compound on (symbol, datetime) per collection
#
# Usage:
#   python historical_preload.py                   # all symbols, all TFs
#   python historical_preload.py --symbol NSE:RELIANCE-EQ
#   python historical_preload.py --tf 5            # only one TF
# ====================================================================

from __future__ import annotations

import argparse
import sys
import time
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

from config import (
    IST,
    FETCH_DAYS,                  # default 5; we override with 30 here
    ASYNC_MAX_CONCURRENT,
    HISTORICAL_PRELOAD_DAYS,     # NEW: 30  (add to config.py)
    HISTORICAL_TIMEFRAMES,       # NEW: ["1","3","5","10","15","30","60"]
    MONGO_URI,                   # NEW: "mongodb://localhost:27017"
    MONGO_DB,                    # NEW: "ema9_wave"
)
from fyers_client import get_fyers, check_token_mid_run
from symbol_loader import load_symbols

# ── MongoDB helpers ───────────────────────────────────────────────────────────

def _get_collection(tf: str):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[f"candles_{tf}"]
    col.create_index(
        [("symbol", ASCENDING), ("datetime", ASCENDING)],
        unique=True,
        background=True,
    )
    return col


def _upsert_candles(col, symbol: str, records: list[dict]) -> int:
    """
    Bulk-upsert candle records.  Skips duplicates silently.
    Returns the number of newly inserted docs.
    """
    if not records:
        return 0
    from pymongo import UpdateOne
    ops = [
        UpdateOne(
            {"symbol": r["symbol"], "datetime": r["datetime"]},
            {"$setOnInsert": r},
            upsert=True,
        )
        for r in records
    ]
    try:
        result = col.bulk_write(ops, ordered=False)
        return result.upserted_count
    except BulkWriteError as bwe:
        # Duplicate key errors are expected and harmless
        return bwe.details.get("nUpserted", 0)


# ── Fetch one (symbol, tf) block ─────────────────────────────────────────────

def _fetch_one(
    fyers,
    symbol: str,
    tf: str,
    days: int,
) -> list[dict] | None:
    """
    Calls Fyers history API and returns a list of candle dicts.
    Returns None on unrecoverable error; empty list if no data.
    """
    now_ist = datetime.now(IST)
    range_to   = now_ist.strftime("%Y-%m-%d")
    range_from = (now_ist - timedelta(days=days + 5)).strftime("%Y-%m-%d")
    # +5 to guarantee we always capture full 30 trading days despite weekends

    try:
        resp = fyers.history({
            "symbol":      symbol,
            "resolution":  tf,
            "date_format": "1",
            "range_from":  range_from,
            "range_to":    range_to,
            "cont_flag":   "1",
        })
    except Exception as exc:
        print(f"  [{symbol}|{tf}m] Request error: {exc}")
        return None

    if resp.get("s") != "ok":
        if check_token_mid_run(resp):
            return None  # caller handles token refresh
        code = str(resp.get("code", ""))
        if code in ("-300", "300"):
            return []   # symbol has no data for this TF
        print(f"  [{symbol}|{tf}m] API error {code}: {resp.get('message','')}")
        return []

    candles = resp.get("candles", [])
    if not candles:
        return []

    # Convert to UTC-aware datetimes for MongoDB
    records: list[dict] = []
    for c in candles:
        ts, o, h, l, cl, v = c
        dt_ist = datetime.fromtimestamp(ts, tz=IST)
        dt_utc = dt_ist.astimezone(tz=None).replace(tzinfo=None)  # naive UTC for Mongo
        records.append({
            "symbol":   symbol,
            "datetime": dt_utc,
            "open":     float(o),
            "high":     float(h),
            "low":      float(l),
            "close":    float(cl),
            "volume":   int(v),
        })

    # Keep only the last `days` calendar days of data
    cutoff = datetime.utcnow() - timedelta(days=days + 1)
    records = [r for r in records if r["datetime"] >= cutoff]
    return records


# ── Worker (runs in thread pool) ─────────────────────────────────────────────

def _worker(
    fyers,
    symbol: str,
    tf: str,
    days: int,
    results: dict,        # {tf: {symbol: count}}
    lock: threading.Lock,
    token_flag: list,     # [False]
) -> None:
    records = _fetch_one(fyers, symbol, tf, days)

    if records is None:
        # Likely token expiry
        token_flag[0] = True
        return

    col = _get_collection(tf)
    inserted = _upsert_candles(col, symbol, records)

    with lock:
        results.setdefault(tf, {})[symbol] = inserted


# ── Async orchestrator ───────────────────────────────────────────────────────

async def _run_preload(
    fyers,
    symbols: list[str],
    timeframes: list[str],
    days: int,
) -> bool:
    """
    Schedules all (symbol × tf) fetch tasks concurrently.
    Returns True if a token expiry was detected.
    """
    tasks: list[tuple[str, str]] = [
        (sym, tf) for sym in symbols for tf in timeframes
    ]
    total = len(tasks)
    done_ctr = [0]

    results: dict   = {}
    lock            = threading.Lock()
    token_flag      = [False]
    sem             = asyncio.Semaphore(ASYNC_MAX_CONCURRENT)
    loop            = asyncio.get_running_loop()
    executor        = ThreadPoolExecutor(max_workers=ASYNC_MAX_CONCURRENT)

    async def _one(symbol: str, tf: str):
        async with sem:
            await loop.run_in_executor(
                executor,
                _worker,
                fyers, symbol, tf, days, results, lock, token_flag,
            )
            with lock:
                done_ctr[0] += 1
            pct = done_ctr[0] * 100 // total
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            sys.stdout.write(f"\r  [{bar}] {done_ctr[0]}/{total} ({pct}%)")
            sys.stdout.flush()

    await asyncio.gather(*[_one(s, tf) for s, tf in tasks])
    sys.stdout.write("\n")
    executor.shutdown(wait=False)

    # Summary
    print(f"\n  Preload summary:")
    for tf in timeframes:
        tf_res = results.get(tf, {})
        newly = sum(tf_res.values())
        print(f"    TF {tf:>3}m  — {len(tf_res)} symbols  |  {newly:>6} new candles stored")

    return token_flag[0]


# ── Public entry point ───────────────────────────────────────────────────────

def run_historical_preload(
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    days: int | None = None,
) -> None:
    """
    Main callable.  Downloads and stores historical candles for every
    (symbol, tf) pair.  Refreshes token automatically if expired.

    Parameters
    ----------
    symbols    : list of Fyers symbol strings; None → load from config
    timeframes : list of resolution strings; None → HISTORICAL_TIMEFRAMES
    days       : calendar days to fetch; None → HISTORICAL_PRELOAD_DAYS
    """
    if symbols is None:
        symbols = load_symbols()
    if timeframes is None:
        timeframes = HISTORICAL_TIMEFRAMES
    if days is None:
        days = HISTORICAL_PRELOAD_DAYS

    fyers = get_fyers()

    print(f"\n  ═══ Historical Preload ═══")
    print(f"  Symbols     : {len(symbols)}")
    print(f"  Timeframes  : {timeframes}")
    print(f"  Days        : {days}")
    print(f"  Mongo       : {MONGO_URI}/{MONGO_DB}")
    print(f"  Total tasks : {len(symbols) * len(timeframes)}")
    print()

    token_expired = asyncio.run(_run_preload(fyers, symbols, timeframes, days))

    if token_expired:
        print("  Token expired mid-run — refreshing and retrying once…")
        fyers = get_fyers()
        asyncio.run(_run_preload(fyers, symbols, timeframes, days))

    print("\n  ✅ Historical preload complete.\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="EMA9 Wave — Historical Preloader")
    parser.add_argument(
        "--symbol", type=str, default=None,
        help="Single Fyers symbol to fetch (e.g. NSE:RELIANCE-EQ)"
    )
    parser.add_argument(
        "--tf", type=str, default=None,
        help="Single timeframe to fetch (e.g. 5)"
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="Number of calendar days to fetch (default: HISTORICAL_PRELOAD_DAYS)"
    )
    args = parser.parse_args()

    syms = [args.symbol] if args.symbol else None
    tfs  = [args.tf]     if args.tf     else None

    run_historical_preload(symbols=syms, timeframes=tfs, days=args.days)


if __name__ == "__main__":
    _cli()