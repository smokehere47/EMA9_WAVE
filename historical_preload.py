# ================= HISTORICAL PRELOAD — EMA 9 WAVE =================
#
# Fetches OHLCV history from Fyers REST API for every symbol × TF.
# Uses the proven batch pattern: BATCH_SIZE symbols in parallel,
# BATCH_PAUSE seconds between batches — same logic as the original scanner.
#
# MongoDB layout:
#   DB  : EMA_wave
#   Collection per TF:  candle_{tf}
#   e.g. candle_1, candle_5, candle_15, candle_60
#   Each collection holds ALL symbols for that timeframe.
#   DOC : { symbol, datetime (IST naive), open, high, low, close, volume }
#   IDX : unique compound on (symbol, datetime) per collection
#
# Structure mirrors:
#   EMA_wave
#   ├── candle_1
#   │   ├── { symbol: "NSE:HINDPETRO-EQ", datetime: ..., open: ..., ... }
#   │   ├── { symbol: "NSE:ALKEM-EQ",     datetime: ..., ... }
#   │   └── ...
#   ├── candle_15
#   │   └── ...
#   └── ...
#
# Usage:
#   python historical_preload.py
#   python historical_preload.py --symbol NSE:RELIANCE-EQ
#   python historical_preload.py --tf 5
#   python historical_preload.py --days 30
# ====================================================================

from __future__ import annotations

import argparse
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

from config import (
    IST,
    HISTORICAL_PRELOAD_DAYS,
    HISTORICAL_TIMEFRAMES,
    MONGO_CREDS_FILE,
    MONGO_DB_DEFAULT,
    MONGO_COLLECTION_PREFIX,
    PRELOAD_BATCH_SIZE,
    PRELOAD_BATCH_PAUSE,
)
from fyers_client import get_fyers, check_token_mid_run
from symbol_loader import load_symbols


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB credentials loaded from file (no creds in source code)
# ─────────────────────────────────────────────────────────────────────────────

def _load_mongo_creds(creds_file: str) -> tuple[str, str]:
    import os
    creds_path = os.path.abspath(creds_file)
    if not os.path.isfile(creds_path):
        raise FileNotFoundError(
            f"MongoDB credentials file not found: {creds_path}\n"
            "Create the file with:\n  uri=mongodb://<host>:<port>\n  db=<database_name>"
        )
    parsed: dict[str, str] = {}
    with open(creds_path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            parsed[key.strip().lower()] = value.strip()
    uri = parsed.get("uri", "")
    if not uri:
        raise ValueError(f"'uri' key missing in credentials file: {creds_path}")
    db_name = parsed.get("db", MONGO_DB_DEFAULT)
    return uri, db_name


try:
    _MONGO_URI, _MONGO_DB = _load_mongo_creds(MONGO_CREDS_FILE)
except (FileNotFoundError, ValueError) as _e:
    print(f"\n  ❌ {_e}\n")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Collection naming
# ─────────────────────────────────────────────────────────────────────────────

def _col_name(tf: str) -> str:
    """
    Collection name pattern: candle_{tf}
    Examples: candle_1, candle_5, candle_15, candle_60
    All symbols for a given TF are stored in the same collection.
    """
    return f"{MONGO_COLLECTION_PREFIX}_{tf}"


# ── Shared MongoDB client (thread-safe, connection-pooled) ────────────────────
_mongo_client: MongoClient | None = None
_mongo_lock   = threading.Lock()


def _get_client() -> MongoClient:
    global _mongo_client
    with _mongo_lock:
        if _mongo_client is None:
            _mongo_client = MongoClient(_MONGO_URI, maxPoolSize=PRELOAD_BATCH_SIZE + 2)
    return _mongo_client


def _get_col(tf: str):
    """
    Return (and auto-create compound index on) the per-TF collection.
    Collection name: candle_{tf}
    Index: unique compound on (symbol, datetime).
    """
    col = _get_client()[_MONGO_DB][_col_name(tf)]
    col.create_index(
        [("symbol", ASCENDING), ("datetime", ASCENDING)],
        unique=True,
        background=True,
    )
    return col


def _upsert(col, records: list[dict]) -> int:
    if not records:
        return 0
    ops = [
        UpdateOne(
            {"symbol": r["symbol"], "datetime": r["datetime"]},
            {"$setOnInsert": r},
            upsert=True,
        )
        for r in records
    ]
    try:
        return col.bulk_write(ops, ordered=False).upserted_count
    except BulkWriteError as bwe:
        return bwe.details.get("nUpserted", 0)


# ── Fyers fetch for one (symbol, tf) ─────────────────────────────────────────

def _fetch(fyers, symbol: str, tf: str, days: int) -> list[dict] | None:
    """
    Returns list[dict] on success, None on token expiry, [] on skip.
    Each document includes 'symbol' field since all symbols share one collection.
    """
    now        = datetime.now(IST)
    range_to   = now.strftime("%Y-%m-%d")
    range_from = (now - timedelta(days=days + 5)).strftime("%Y-%m-%d")

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
        print(f"\n  [{symbol}|{tf}m] error: {exc}")
        return []

    s    = resp.get("s", "")
    code = str(resp.get("code", ""))

    if s != "ok":
        if check_token_mid_run(resp):
            return None
        if code not in ("-300", "300"):
            print(f"\n  [{symbol}|{tf}m] API {code}: {resp.get('message','')}")
        return []

    candles = resp.get("candles", [])
    if not candles:
        return []

    cutoff = datetime.now(IST).replace(tzinfo=None) - timedelta(days=days + 1)
    records: list[dict] = []
    for ts, o, h, l, cl, v in candles:
        dt_ist = datetime.fromtimestamp(ts, tz=IST).replace(tzinfo=None)
        if dt_ist < cutoff:
            continue
        records.append({
            "symbol":   symbol,          # stored in document — collection is shared
            "datetime": dt_ist,          # IST naive — strategy reads with tz_localize(IST)
            "open":     float(o),
            "high":     float(h),
            "low":      float(l),
            "close":    float(cl),
            "volume":   int(v),
        })
    return records


# ── Worker: fetch + upsert (runs inside thread pool) ─────────────────────────

def _fetch_and_store(fyers, symbol: str, tf: str, days: int) -> tuple[str, int | None]:
    """Returns (symbol, inserted_count) or (symbol, None) on token expiry."""
    col     = _get_col(tf)
    records = _fetch(fyers, symbol, tf, days)
    if records is None:
        return symbol, None
    return symbol, _upsert(col, records)


# ── Core preload: batch pattern identical to original scanner ─────────────────

def _do_preload(fyers, symbols: list[str], timeframes: list[str], days: int) -> bool:
    """
    For each TF: split symbols into batches of PRELOAD_BATCH_SIZE,
    fetch each batch in parallel, pause PRELOAD_BATCH_PAUSE between batches.

    All symbols for a given TF are written to one collection: candle_{tf}
    Documents include a 'symbol' field; index is compound (symbol, datetime).

    Returns True if token expired.
    """
    total   = len(symbols) * len(timeframes)
    done    = 0
    summary: dict[str, int] = {tf: 0 for tf in timeframes}

    for tf in timeframes:
        num_batches = (len(symbols) + PRELOAD_BATCH_SIZE - 1) // PRELOAD_BATCH_SIZE
        print(f"\n  ── TF {tf}m ── ({len(symbols)} symbols, {num_batches} batches)")
        print(f"     Collection: {_col_name(tf)}  (all symbols in one collection)")

        for batch_num, start in enumerate(range(0, len(symbols), PRELOAD_BATCH_SIZE), 1):
            batch = symbols[start : start + PRELOAD_BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=PRELOAD_BATCH_SIZE) as ex:
                futures = {
                    ex.submit(_fetch_and_store, fyers, sym, tf, days): sym
                    for sym in batch
                }
                for fut in as_completed(futures):
                    sym, inserted = fut.result()
                    if inserted is None:          # token expired
                        return True
                    summary[tf]  += inserted
                    done         += 1
                    pct = done * 100 // total
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    sys.stdout.write(
                        f"\r  [{bar}] {done}/{total} ({pct}%)  "
                        f"batch {batch_num}/{num_batches}  {sym[:24]:<24}"
                    )
                    sys.stdout.flush()

            # Pause between batches (not after the last batch of a TF)
            if batch_num < num_batches:
                time.sleep(PRELOAD_BATCH_PAUSE)

    sys.stdout.write("\n")
    print(f"\n  Preload summary:")
    for tf in timeframes:
        print(f"    TF {tf:>3}m — {summary[tf]:>6} new candles stored  →  collection: {_col_name(tf)}")
    return False


# ── Public entry point ────────────────────────────────────────────────────────

def run_historical_preload(
    symbols:    list[str] | None = None,
    timeframes: list[str] | None = None,
    days:       int | None = None,
) -> None:
    if symbols    is None: symbols    = load_symbols()
    if timeframes is None: timeframes = HISTORICAL_TIMEFRAMES
    if days       is None: days       = HISTORICAL_PRELOAD_DAYS

    fyers = get_fyers()

    print(f"\n  ═══ Historical Preload ═══")
    print(f"  Symbols     : {len(symbols)}")
    print(f"  Timeframes  : {timeframes}")
    print(f"  Days        : {days}")
    print(f"  Batch size  : {PRELOAD_BATCH_SIZE}  |  Batch pause: {PRELOAD_BATCH_PAUSE}s")
    print(f"  Mongo DB    : {_MONGO_DB}")
    print(f"  Layout      : candle_{{tf}}  (one collection per TF, all symbols inside)")
    print(f"  Total tasks : {len(symbols) * len(timeframes)}")
    print()

    expired = _do_preload(fyers, symbols, timeframes, days)

    if expired:
        print("  Token expired — refreshing and retrying…")
        fyers = get_fyers()
        _do_preload(fyers, symbols, timeframes, days)

    print("\n  ✅ Historical preload complete.\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EMA9 Wave — Historical Preloader")
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--tf",     type=str, default=None)
    parser.add_argument("--days",   type=int, default=None)
    args = parser.parse_args()

    run_historical_preload(
        symbols    = [args.symbol] if args.symbol else None,
        timeframes = [args.tf]     if args.tf     else None,
        days       = args.days,
    )