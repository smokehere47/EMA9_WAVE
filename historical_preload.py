# ================= HISTORICAL PRELOAD — EMA 9 WAVE =================
#
# Fetches OHLCV history from Fyers REST API for every symbol × TF.
#
# Key behaviours
# ──────────────
# 1. TRADING-DAY WINDOW
#    HISTORICAL_PRELOAD_DAYS is in TRADING days (not calendar days).
#    The NSE trading calendar is sourced from time_utils.py (single source
#    of truth for holidays and trading-day helpers — no duplication here).
#
# 2. INCREMENTAL / ROLLING FETCH
#    For each (symbol, tf) the code checks the newest datetime already in
#    MongoDB.  Only candles newer than that date are fetched from Fyers.
#    This means on a normal day only 1–2 days of data are pulled instead
#    of the full 45-day window.
#
# 3. ROLLING WINDOW PRUNE
#    After inserting new candles, any documents older than the rolling
#    window cutoff (= today − HISTORICAL_PRELOAD_DAYS trading days) are
#    deleted.  The stored window stays exactly HISTORICAL_PRELOAD_DAYS
#    trading days deep regardless of how long the system has been running.
#
# 4. COMPOUND INDEX
#    Each candle_{tf} collection has a unique compound index on
#    (symbol, datetime).  This makes per-symbol range queries fast for
#    both backtesting and live scanning.
#
# MongoDB layout:
#   DB  : EMA_wave
#   Collection per TF:  candle_{tf}   e.g. candle_1, candle_15, candle_60
#   DOC : { symbol, datetime (IST naive), open, high, low, close, volume }
#   IDX : unique compound (symbol ASC, datetime ASC)
#
# Usage:
#   python historical_preload.py
#   python historical_preload.py --symbol NSE:RELIANCE-EQ
#   python historical_preload.py --tf 5
# ====================================================================

from __future__ import annotations

import argparse
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta

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
from time_utils import is_trading_day, get_last_trading_day, trading_days_ago


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB credentials loaded from file (no creds in source code)
# ─────────────────────────────────────────────────────────────────────────────

def _load_mongo_creds(creds_file: str) -> tuple[str, str]:
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
    """candle_1, candle_5, candle_15, candle_60 …"""
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
    Return the per-TF collection with a guaranteed unique compound index
    on (symbol ASC, datetime ASC).  Fast for per-symbol range queries.
    """
    col = _get_client()[_MONGO_DB][_col_name(tf)]
    col.create_index(
        [("symbol", ASCENDING), ("datetime", ASCENDING)],
        unique=True,
        background=True,
    )
    return col


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol newest-stored-date query
# ─────────────────────────────────────────────────────────────────────────────

def _newest_stored_dt(col, symbol: str) -> datetime | None:
    """
    Return the datetime of the most recent candle stored for `symbol` in `col`,
    or None if the symbol has no data yet.
    """
    doc = col.find_one(
        {"symbol": symbol},
        {"_id": 0, "datetime": 1},
        sort=[("datetime", -1)],
    )
    return doc["datetime"] if doc else None


# ─────────────────────────────────────────────────────────────────────────────
# Fyers fetch
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(
    fyers,
    symbol:     str,
    tf:         str,
    range_from: date,
    range_to:   date,
) -> list[dict] | None:
    """
    Fetch candles for `symbol` / `tf` between `range_from` and `range_to`
    (both inclusive, as calendar dates).

    Returns:
        list[dict]  — records ready to upsert (may be empty)
        None        — token expired; caller should refresh and retry
    """
    try:
        resp = fyers.history({
            "symbol":      symbol,
            "resolution":  tf,
            "date_format": "1",
            "range_from":  range_from.strftime("%Y-%m-%d"),
            "range_to":    range_to.strftime("%Y-%m-%d"),
            "cont_flag":   "1",
        })
    except Exception as exc:
        print(f"\n  [{symbol}|{tf}m] fetch error: {exc}")
        return []

    s    = resp.get("s", "")
    code = str(resp.get("code", ""))

    if s != "ok":
        if check_token_mid_run(resp):
            return None
        if code not in ("-300", "300"):
            print(f"\n  [{symbol}|{tf}m] API {code}: {resp.get('message', '')}")
        return []

    candles = resp.get("candles", [])
    if not candles:
        return []

    records: list[dict] = []
    for ts, o, h, l, cl, v in candles:
        dt_ist = datetime.fromtimestamp(ts, tz=IST).replace(tzinfo=None)
        records.append({
            "symbol":   symbol,
            "datetime": dt_ist,   # IST naive — strategy reads with tz_localize(IST)
            "open":     float(o),
            "high":     float(h),
            "low":      float(l),
            "close":    float(cl),
            "volume":   int(v),
        })
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Upsert helpers
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Prune: delete candles older than the rolling window cutoff
# ─────────────────────────────────────────────────────────────────────────────

def _prune(col, symbol: str, cutoff_dt: datetime) -> int:
    """
    Delete all documents for `symbol` with datetime < `cutoff_dt`.
    Returns the number of documents deleted.
    """
    result = col.delete_many({
        "symbol":   symbol,
        "datetime": {"$lt": cutoff_dt},
    })
    return result.deleted_count


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol incremental worker
# ─────────────────────────────────────────────────────────────────────────────

def _sync_symbol(
    fyers,
    symbol:       str,
    tf:           str,
    window_start: date,    # oldest trading day we want to keep  (cutoff date)
    today:        date,    # newest trading day (run-day)
) -> tuple[str, int, int] | None:
    """
    Incremental sync for one (symbol, tf):
      1. Find the newest datetime already stored.
      2. Fetch only the missing range (newest_stored+1 → today).
         If nothing stored yet, fetch the full window (window_start → today).
      3. Upsert new candles.
      4. Prune candles older than window_start.

    Returns (symbol, inserted, pruned) or None on token expiry.
    """
    col         = _get_col(tf)
    newest_dt   = _newest_stored_dt(col, symbol)

    # Determine what date range to fetch
    if newest_dt is None:
        # No data at all — full window fetch
        fetch_from = window_start
    else:
        newest_date = newest_dt.date()
        if newest_date >= today:
            # Already up to date — still prune in case window shrank
            fetch_from = None
        else:
            # Fetch the missing days: day after newest stored → today
            fetch_from = newest_date + timedelta(days=1)

    inserted = 0
    if fetch_from is not None:
        records = _fetch(fyers, symbol, tf, fetch_from, today)
        if records is None:
            return None   # token expired
        inserted = _upsert(col, records)

    # Prune: delete anything before window_start (as IST-naive datetime)
    cutoff_dt = datetime.combine(window_start, datetime.min.time())
    pruned    = _prune(col, symbol, cutoff_dt)

    return symbol, inserted, pruned


# ─────────────────────────────────────────────────────────────────────────────
# Core preload: batch pattern
# ─────────────────────────────────────────────────────────────────────────────

def _do_preload(
    fyers,
    symbols:      list[str],
    timeframes:   list[str],
    trading_days: int,
) -> bool:
    """
    For each TF: split symbols into batches, sync each symbol incrementally.
    Returns True if a token expiry was detected.
    """
    today        = get_last_trading_day(datetime.now())
    window_start = trading_days_ago(trading_days - 1, from_date=today)
    # window_start is the first day we want to KEEP; anything before is pruned.
    # Example: today=Mon Apr 28, trading_days=45 → window_start = ~Feb 24

    total   = len(symbols) * len(timeframes)
    done    = 0
    summary: dict[str, dict] = {tf: {"inserted": 0, "pruned": 0} for tf in timeframes}

    for tf in timeframes:
        num_batches = (len(symbols) + PRELOAD_BATCH_SIZE - 1) // PRELOAD_BATCH_SIZE
        print(f"\n  ── TF {tf}m ── ({len(symbols)} symbols, {num_batches} batches)")
        print(f"     Collection : {_col_name(tf)}")
        print(f"     Window     : {window_start} → {today}  ({trading_days} trading days)")

        for batch_num, start in enumerate(range(0, len(symbols), PRELOAD_BATCH_SIZE), 1):
            batch = symbols[start : start + PRELOAD_BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=PRELOAD_BATCH_SIZE) as ex:
                futures = {
                    ex.submit(_sync_symbol, fyers, sym, tf, window_start, today): sym
                    for sym in batch
                }
                for fut in as_completed(futures):
                    result = fut.result()
                    if result is None:
                        return True   # token expired
                    sym, inserted, pruned = result
                    summary[tf]["inserted"] += inserted
                    summary[tf]["pruned"]   += pruned
                    done += 1
                    pct = done * 100 // total
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    sys.stdout.write(
                        f"\r  [{bar}] {done}/{total} ({pct}%)  "
                        f"batch {batch_num}/{num_batches}  "
                        f"+{inserted} pruned {pruned}  {sym[:22]:<22}"
                    )
                    sys.stdout.flush()

            if batch_num < num_batches:
                time.sleep(PRELOAD_BATCH_PAUSE)

    sys.stdout.write("\n")
    print(f"\n  Preload summary:")
    for tf in timeframes:
        ins = summary[tf]["inserted"]
        prn = summary[tf]["pruned"]
        print(f"    TF {tf:>3}m — {ins:>6} inserted   {prn:>6} pruned   →  {_col_name(tf)}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_historical_preload(
    symbols:      list[str] | None = None,
    timeframes:   list[str] | None = None,
    trading_days: int | None = None,
) -> None:
    if symbols      is None: symbols      = load_symbols()
    if timeframes   is None: timeframes   = HISTORICAL_TIMEFRAMES
    if trading_days is None: trading_days = HISTORICAL_PRELOAD_DAYS

    fyers = get_fyers()

    today        = get_last_trading_day(datetime.now())
    window_start = trading_days_ago(trading_days - 1, from_date=today)

    print(f"\n  ═══ Historical Preload (Incremental Rolling Window) ═══")
    print(f"  Symbols       : {len(symbols)}")
    print(f"  Timeframes    : {timeframes}")
    print(f"  Window        : {trading_days} trading days  ({window_start} → {today})")
    print(f"  Batch size    : {PRELOAD_BATCH_SIZE}  |  Batch pause: {PRELOAD_BATCH_PAUSE}s")
    print(f"  Mongo DB      : {_MONGO_DB}")
    print(f"  Layout        : candle_{{tf}}  (one collection per TF, all symbols inside)")
    print(f"  Total tasks   : {len(symbols) * len(timeframes)}")
    print(f"  Mode          : incremental — only missing days fetched, old days pruned")
    print()

    expired = _do_preload(fyers, symbols, timeframes, trading_days)

    if expired:
        print("\n  Token expired — refreshing and retrying full batch…")
        fyers = get_fyers()
        _do_preload(fyers, symbols, timeframes, trading_days)

    print("\n  ✅ Historical preload complete.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EMA9 Wave — Incremental Historical Preloader")
    parser.add_argument("--symbol", type=str, default=None,
                        help="Single symbol to sync, e.g. NSE:RELIANCE-EQ")
    parser.add_argument("--tf",     type=str, default=None,
                        help="Single timeframe to sync, e.g. 15")
    parser.add_argument("--days",   type=int, default=None,
                        help="Override HISTORICAL_PRELOAD_DAYS (trading days)")
    args = parser.parse_args()

    run_historical_preload(
        symbols      = [args.symbol] if args.symbol else None,
        timeframes   = [args.tf]     if args.tf     else None,
        trading_days = args.days,
    )