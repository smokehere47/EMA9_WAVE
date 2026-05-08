# ================= HISTORICAL PRELOAD — EMA 9 WAVE =================
#
# TIMEZONE POLICY (read carefully):
# ──────────────────────────────────
#   ALL datetimes stored in MongoDB are IST NAIVE.
#   "IST naive" = a datetime object with NO tzinfo attached,
#   but whose value is already in Asia/Kolkata local time.
#
#   Example stored value: 2026-05-07 09:15:00  (no Z, no +05:30)
#   This represents 09:15 IST, which is what the candle actually is.
#
#   WHY IST NAIVE (not UTC):
#   • This is a India-only system. UTC adds zero value.
#   • IST naive is the simplest possible format — no conversions needed.
#   • Fyers gives Unix timestamps → convert once to IST naive → store.
#   • WebSocket does the same → store → no mismatch ever.
#   • Reader just does tz_localize(IST) to make pandas timezone-aware.
#
#   PIPELINE FLOW:
#     Fyers Unix ts  →  datetime.fromtimestamp(ts, tz=IST).replace(tzinfo=None)
#                     =  IST naive  →  MongoDB
#     MongoDB        →  pd.to_datetime(docs).dt.tz_localize(IST)
#                     =  IST-aware pandas column  →  strategy
#
#   CANDLE VALIDATION:
#   • NSE 3-min session: 09:15 → 15:27 = 125 candles/day
#   • NSE 1-min session: 09:15 → 15:29 = 375 candles/day
#   • After fetch, days with < MIN_CANDLE_FRACTION of expected are refetched.
#   • Duplicate candles are removed by the unique compound index.
#
#   DYNAMIC REPAIR:
#   • Scans every trading day in the full window (not just from newest date).
#   • Detects: missing candles, duplicates, corrupted timestamps,
#     incorrect intervals, incomplete day counts.
#   • Repairs only the specific (symbol, tf, day) that is broken.
#   • Does NOT delete or refetch complete history unnecessarily.
#
# MongoDB layout:
#   DB  : EMA9_WAVE
#   Collection per TF : candle_{tf}   e.g. candle_1, candle_3, candle_15
#   DOC : { symbol, datetime (IST naive), open, high, low, close, volume }
#   IDX : unique compound (symbol ASC, datetime ASC)
#
# Usage:
#   python historical_preload.py
#   python historical_preload.py --symbol NSE:RELIANCE-EQ
#   python historical_preload.py --tf 5
#   python historical_preload.py --months 3
#   python historical_preload.py --validate          # deep validate + fix
#   python historical_preload.py --clean-reload      # drop + reload all data fresh
#   python historical_preload.py --repair-date 2026-05-12   # repair one specific date
# ====================================================================

from __future__ import annotations

import argparse
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, time as dtime
from collections import defaultdict
from typing import Optional

import pytz
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient, ASCENDING, UpdateOne, DeleteMany
from pymongo.errors import BulkWriteError

from config import (
    IST,
    HISTORICAL_PRELOAD_MONTHS,
    HISTORICAL_TIMEFRAMES,
    MONGO_CREDS_FILE,
    MONGO_DB_DEFAULT,
    MONGO_COLLECTION_PREFIX,
    PRELOAD_BATCH_SIZE,
    PRELOAD_BATCH_PAUSE,
)
from fyers_client import get_fyers, check_token_mid_run
from symbol_loader import load_symbols
from time_utils import get_last_trading_day, is_trading_day


# ─────────────────────────────────────────────────────────────────────────────
# NSE session constants
# ─────────────────────────────────────────────────────────────────────────────

_MARKET_OPEN  = dtime(9, 15)    # 09:15 IST
_MARKET_CLOSE = dtime(15, 30)   # 15:30 IST  (last candle opens at 15:27 for 3m)

# Minimum fraction of expected candles for a day to be considered complete.
_MIN_CANDLE_FRACTION = 0.90

# Maximum allowed gap (in minutes × 1.5) between consecutive candles on same day.
# Helps detect corrupted timestamp jumps.
_MAX_INTERVAL_MULTIPLIER = 1.5


def expected_candles_per_day(tf_minutes: int) -> int:
    """
    Total candles in one full NSE trading session for the given timeframe.
    Session: 09:15 → 15:30 = 375 minutes.
    """
    session_minutes = 375   # 09:15 to 15:30
    return session_minutes // tf_minutes


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB credentials
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
# Collection naming & MongoDB client
# ─────────────────────────────────────────────────────────────────────────────

def _col_name(tf: str) -> str:
    return f"{MONGO_COLLECTION_PREFIX}_{tf}"


_mongo_client: MongoClient | None = None
_mongo_lock   = threading.Lock()


def _get_client() -> MongoClient:
    global _mongo_client
    with _mongo_lock:
        if _mongo_client is None:
            _mongo_client = MongoClient(_MONGO_URI, maxPoolSize=PRELOAD_BATCH_SIZE + 2)
    return _mongo_client


def _get_col(tf: str):
    """Return the per-TF collection, ensuring the compound unique index exists."""
    col = _get_client()[_MONGO_DB][_col_name(tf)]
    col.create_index(
        [("symbol", ASCENDING), ("datetime", ASCENDING)],
        unique=True,
        background=True,
        name="symbol_datetime_unique",
    )
    return col


# ─────────────────────────────────────────────────────────────────────────────
# IST naive datetime helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ts_to_ist_naive(unix_ts: int) -> datetime:
    """
    Convert a Unix timestamp to an IST naive datetime.
    This is the CANONICAL conversion used everywhere in this codebase.
    """
    return datetime.fromtimestamp(unix_ts, tz=IST).replace(tzinfo=None)


def _ist_naive_day_range(d: date) -> tuple[datetime, datetime]:
    """
    Return (day_start, day_end) as IST naive datetimes for a given date.
    day_start = 09:15:00, day_end = 15:30:00 IST naive.
    """
    day_start = datetime.combine(d, _MARKET_OPEN)
    day_end   = datetime.combine(d, _MARKET_CLOSE)
    return day_start, day_end


def _initial_window_start(today: date, months: int) -> date:
    """Return today − months calendar months."""
    return (datetime.combine(today, datetime.min.time()) - relativedelta(months=months)).date()


# ─────────────────────────────────────────────────────────────────────────────
# Newest / oldest stored IST naive date query
# ─────────────────────────────────────────────────────────────────────────────

def _newest_stored_date(col, symbol: str) -> date | None:
    doc = col.find_one(
        {"symbol": symbol},
        {"_id": 0, "datetime": 1},
        sort=[("datetime", -1)],
        hint=[("symbol", ASCENDING), ("datetime", ASCENDING)],
    )
    if doc is None:
        return None
    return doc["datetime"].date()


def _oldest_stored_date(col, symbol: str) -> date | None:
    doc = col.find_one(
        {"symbol": symbol},
        {"_id": 0, "datetime": 1},
        sort=[("datetime", ASCENDING)],
        hint=[("symbol", ASCENDING), ("datetime", ASCENDING)],
    )
    if doc is None:
        return None
    return doc["datetime"].date()


# ─────────────────────────────────────────────────────────────────────────────
# Candle validation — detect all problem types for a single (symbol, tf, day)
# ─────────────────────────────────────────────────────────────────────────────

class DayAudit:
    """
    Result of auditing one (symbol, tf, date).
    A day is flagged for repair if ANY issue is found.
    """
    __slots__ = (
        "symbol", "tf", "day",
        "candle_count", "expected",
        "has_missing", "has_duplicate", "has_corrupt_ts", "has_bad_interval",
        "issues",
    )

    def __init__(
        self,
        symbol:       str,
        tf:           str,
        day:          date,
        candle_count: int,
        expected:     int,
    ):
        self.symbol       = symbol
        self.tf           = tf
        self.day          = day
        self.candle_count = candle_count
        self.expected     = expected
        self.has_missing      = False
        self.has_duplicate    = False
        self.has_corrupt_ts   = False
        self.has_bad_interval = False
        self.issues: list[str] = []

    @property
    def needs_repair(self) -> bool:
        return bool(self.issues)

    def flag(self, kind: str, detail: str = "") -> None:
        label = f"{kind}: {detail}" if detail else kind
        if label not in self.issues:
            self.issues.append(label)


def _audit_day(
    col,
    symbol:  str,
    tf:      str,
    day:     date,
    tf_int:  int,
) -> DayAudit:
    """
    Fetch all candles for (symbol, tf, day) and check for:
      1. Missing candles (count below threshold)
      2. Duplicate datetimes
      3. Corrupted timestamps (outside session window)
      4. Incorrect intervals between consecutive candles
    """
    expected    = expected_candles_per_day(tf_int)
    day_start, day_end = _ist_naive_day_range(day)

    docs = list(col.find(
        {
            "symbol":   symbol,
            "datetime": {"$gte": day_start, "$lte": day_end},
        },
        {"_id": 0, "datetime": 1},
        sort=[("datetime", ASCENDING)],
    ))

    audit = DayAudit(symbol, tf, day, len(docs), expected)
    min_ok = int(expected * _MIN_CANDLE_FRACTION)

    # ── 1. Missing candles ────────────────────────────────────────────────────
    if len(docs) < min_ok:
        audit.has_missing = True
        audit.flag("missing", f"{len(docs)}/{expected} candles")

    if not docs:
        return audit   # nothing more to check

    datetimes = [d["datetime"] for d in docs]

    # ── 2. Duplicate datetimes ────────────────────────────────────────────────
    seen: set[datetime] = set()
    for dt in datetimes:
        if dt in seen:
            audit.has_duplicate = True
            audit.flag("duplicate", str(dt))
            break
        seen.add(dt)

    # ── 3. Corrupted timestamps (outside session window) ──────────────────────
    for dt in datetimes:
        t = dt.time()
        if t < _MARKET_OPEN or t > _MARKET_CLOSE:
            audit.has_corrupt_ts = True
            audit.flag("corrupt_ts", str(dt))
            break
        # Also flag if date is wrong (candle from a different day stored here)
        if dt.date() != day:
            audit.has_corrupt_ts = True
            audit.flag("corrupt_ts", f"wrong date {dt.date()} in {day}")
            break

    # ── 4. Incorrect intervals ────────────────────────────────────────────────
    max_gap = timedelta(minutes=tf_int * _MAX_INTERVAL_MULTIPLIER)
    for i in range(1, len(datetimes)):
        gap = datetimes[i] - datetimes[i - 1]
        # Gap must be positive (already sorted) and not exceed max
        if gap.total_seconds() <= 0:
            audit.has_bad_interval = True
            audit.flag("bad_interval", f"non-positive gap at {datetimes[i]}")
            break
        if gap > max_gap:
            # Allow one large gap at market open (pre-open candles sometimes
            # have a gap from 09:08→09:15); only flag mid-session gaps.
            mid_session = (
                datetimes[i - 1].time() >= _MARKET_OPEN
                and datetimes[i].time()  <= _MARKET_CLOSE
            )
            if mid_session and datetimes[i - 1].time() != dtime(9, 15):
                audit.has_bad_interval = True
                audit.flag(
                    "bad_interval",
                    f"gap {gap} between {datetimes[i-1].time()} and {datetimes[i].time()}",
                )
                break

    return audit


# ─────────────────────────────────────────────────────────────────────────────
# Full dynamic scan — finds ALL broken days across the entire window
# ─────────────────────────────────────────────────────────────────────────────

def _scan_all_days(
    col,
    symbol:      str,
    tf:          str,
    from_date:   date,
    to_date:     date,
    tf_int:      int,
    verbose:     bool = False,
) -> list[DayAudit]:
    """
    Scan EVERY trading day in [from_date, to_date].
    Returns list of DayAudit objects that need repair.

    This is the core of dynamic repair — it does not rely on newest_stored_date;
    it checks every historical day individually.
    """
    broken: list[DayAudit] = []
    cur = from_date
    while cur <= to_date:
        if is_trading_day(cur):
            audit = _audit_day(col, symbol, tf, cur, tf_int)
            if audit.needs_repair:
                broken.append(audit)
                if verbose:
                    print(f"      ⚠  {symbol}|{tf}m|{cur}: {', '.join(audit.issues)}")
        cur += timedelta(days=1)
    return broken


# ─────────────────────────────────────────────────────────────────────────────
# Fyers REST fetch
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(
    fyers,
    symbol:     str,
    tf:         str,
    range_from: date,
    range_to:   date,
) -> list[dict] | None:
    """
    Fetch candles from Fyers REST API.
    Returns list of dicts with IST naive datetimes, or None on token expiry.
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
        dt_ist_naive = _ts_to_ist_naive(int(ts))
        t = dt_ist_naive.time()
        if t < _MARKET_OPEN or t > _MARKET_CLOSE:
            continue
        records.append({
            "symbol":   symbol,
            "datetime": dt_ist_naive,
            "open":     float(o),
            "high":     float(h),
            "low":      float(l),
            "close":    float(cl),
            "volume":   int(v),
        })
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Delete candles for specific days
# ─────────────────────────────────────────────────────────────────────────────

def _delete_days(col, symbol: str, days: list[date]) -> int:
    """Delete all candles for a symbol on the given list of IST dates."""
    if not days:
        return 0
    total_deleted = 0
    for d in days:
        day_start, day_end = _ist_naive_day_range(d)
        result = col.delete_many({
            "symbol":   symbol,
            "datetime": {"$gte": day_start, "$lte": day_end},
        })
        total_deleted += result.deleted_count
    return total_deleted


# ─────────────────────────────────────────────────────────────────────────────
# Upsert (insert-only for historical, update for live)
# ─────────────────────────────────────────────────────────────────────────────

def _upsert(col, records: list[dict]) -> int:
    """
    Bulk insert-only upsert using $setOnInsert.
    Existing candles are NEVER modified — only new ones are inserted.
    """
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


def _remove_duplicates(col, symbol: str, tf_int: int, day: date) -> int:
    """
    Remove duplicate (symbol, datetime) documents for a specific day.
    Keeps the first occurrence, deletes subsequent ones.
    Returns count of duplicates removed.
    """
    day_start, day_end = _ist_naive_day_range(day)
    pipeline = [
        {"$match": {"symbol": symbol, "datetime": {"$gte": day_start, "$lte": day_end}}},
        {"$sort":  {"datetime": ASCENDING, "_id": ASCENDING}},
        {"$group": {
            "_id":    "$datetime",
            "ids":    {"$push": "$_id"},
            "count":  {"$sum": 1},
        }},
        {"$match": {"count": {"$gt": 1}}},
    ]
    removed = 0
    for group in col.aggregate(pipeline):
        dup_ids = group["ids"][1:]   # keep first, delete rest
        result  = col.delete_many({"_id": {"$in": dup_ids}})
        removed += result.deleted_count
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# Repair a list of broken DayAudit objects
# ─────────────────────────────────────────────────────────────────────────────

def _repair_audits(
    fyers,
    col,
    symbol:  str,
    tf:      str,
    tf_int:  int,
    audits:  list[DayAudit],
    verbose: bool = True,
) -> tuple[int, int]:
    """
    Repair each broken day:
      - Duplicate-only issues: remove duplicates in-place (no API call needed)
      - Any other issue: delete the day + refetch from Fyers

    Returns (days_repaired, candles_inserted).
    """
    # Separate duplicate-only days from days needing full refetch
    dup_only_days:   list[date] = []
    full_refetch_days: list[date] = []

    for audit in audits:
        only_dup = (
            audit.has_duplicate
            and not audit.has_missing
            and not audit.has_corrupt_ts
            and not audit.has_bad_interval
        )
        if only_dup:
            dup_only_days.append(audit.day)
        else:
            full_refetch_days.append(audit.day)

    # ── Fix duplicate-only days in-place ──────────────────────────────────────
    for d in dup_only_days:
        removed = _remove_duplicates(col, symbol, tf_int, d)
        if verbose:
            print(f"    [{symbol}|{tf}m] {d}: removed {removed} duplicate(s)")

    # ── Full refetch for remaining broken days ────────────────────────────────
    total_inserted = 0
    if full_refetch_days:
        # Group consecutive days for efficient Fyers API calls
        # (one API call per contiguous date range, not one per day)
        full_refetch_days.sort()
        date_groups: list[list[date]] = []
        group: list[date] = [full_refetch_days[0]]

        for d in full_refetch_days[1:]:
            # Extend group if next date is within 7 calendar days (handles weekends)
            if (d - group[-1]).days <= 7:
                group.append(d)
            else:
                date_groups.append(group)
                group = [d]
        date_groups.append(group)

        for grp in date_groups:
            min_day = min(grp)
            max_day = max(grp)
            # Delete all days in this group before refetch
            deleted = _delete_days(col, symbol, grp)
            if verbose:
                print(
                    f"    [{symbol}|{tf}m] refetch {min_day}→{max_day} "
                    f"({len(grp)} day(s), deleted {deleted} stale docs)"
                )
            records = _fetch(fyers, symbol, tf, min_day, max_day)
            if records is None:
                return len(dup_only_days) + len(full_refetch_days), total_inserted   # token expired
            inserted = _upsert(col, records)
            total_inserted += inserted
            if verbose:
                print(f"      → inserted {inserted} candle(s)")

    return len(dup_only_days) + len(full_refetch_days), total_inserted


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol incremental sync
# ─────────────────────────────────────────────────────────────────────────────

def _sync_symbol(
    fyers,
    symbol:       str,
    tf:           str,
    window_start: date,
    today:        date,
    force_days:   list[date] | None = None,
) -> tuple[str, int] | None:
    """
    Incremental sync for one (symbol, tf).

    Decision tree:
      force_days provided  → delete those days and refetch them specifically
      No data in DB        → fetch full window (window_start → today)
      Data exists, stale   → fetch from newest IST date → today
      Already current      → nothing to fetch

    Returns (symbol, inserted_count) or None on token expiry.
    """
    col = _get_col(tf)

    if force_days:
        _delete_days(col, symbol, force_days)
        min_day = min(force_days)
        max_day = max(force_days)
        records = _fetch(fyers, symbol, tf, min_day, max_day)
        if records is None:
            return None
        inserted = _upsert(col, records)
        return symbol, inserted

    newest_date = _newest_stored_date(col, symbol)

    if newest_date is None:
        fetch_from = window_start
    else:
        if newest_date > today:
            return symbol, 0
        # Re-fetch from newest stored date to catch any intra-day gaps.
        # $setOnInsert ensures existing complete candles are not overwritten.
        fetch_from = newest_date

    records = _fetch(fyers, symbol, tf, fetch_from, today)
    if records is None:
        return None

    inserted = _upsert(col, records)
    return symbol, inserted


# ─────────────────────────────────────────────────────────────────────────────
# Deep validate + dynamic repair — scans EVERY day in the window
# ─────────────────────────────────────────────────────────────────────────────

def _validate_and_fix(
    fyers,
    symbols:    list[str],
    timeframes: list[str],
    months:     int,
    verbose:    bool = True,
) -> None:
    """
    Scan the full window for each (symbol, tf), checking EVERY trading day —
    not just from the newest stored date.

    For each broken day, the repair is targeted:
      • Duplicate-only → remove in-place, no API call
      • Missing / corrupt / bad interval → delete + refetch that day only

    This catches issues like:
      • Today=17, data for 12 is missing but 13–16 are fine
      • A single corrupted candle mid-history
      • A duplicate insert that didn't violate the unique index (shouldn't
        happen with the current index, but handled defensively)
    """
    today        = get_last_trading_day(datetime.now(IST))
    window_start = _initial_window_start(today, months)

    print(f"\n  ══ Deep Validation + Dynamic Repair ══")
    print(f"  Scanning every trading day: {window_start} → {today}")
    print(f"  Min candle fraction threshold: {_MIN_CANDLE_FRACTION * 100:.0f}%")
    print(f"  Max interval multiplier: {_MAX_INTERVAL_MULTIPLIER}×\n")

    total_broken   = 0
    total_repaired = 0
    total_inserted = 0

    for tf in timeframes:
        col    = _get_col(tf)
        tf_int = int(tf)
        print(f"  ─ TF {tf}m — expected {expected_candles_per_day(tf_int)} candles/day")

        for symbol in symbols:
            broken_audits = _scan_all_days(
                col, symbol, tf, window_start, today, tf_int, verbose=verbose
            )
            if not broken_audits:
                continue

            total_broken += len(broken_audits)
            if verbose:
                print(f"    [{symbol}|{tf}m] {len(broken_audits)} broken day(s) found")

            repaired, inserted = _repair_audits(
                fyers, col, symbol, tf, tf_int, broken_audits, verbose=verbose
            )
            total_repaired += repaired
            total_inserted += inserted

    print(f"\n  ── Validation Summary ──")
    print(f"  Broken days found   : {total_broken}")
    print(f"  Days repaired       : {total_repaired}")
    print(f"  Candles inserted    : {total_inserted}")
    print(f"\n  ✅ Validation + repair complete.\n")


# ─────────────────────────────────────────────────────────────────────────────
# Repair a single specific date (CLI convenience)
# ─────────────────────────────────────────────────────────────────────────────

def _repair_single_date(
    fyers,
    symbols:    list[str],
    timeframes: list[str],
    target_day: date,
) -> None:
    """
    Force-delete and refetch one specific IST date for all symbols/TFs.
    Useful for targeted repair after a known data outage.
    """
    if not is_trading_day(target_day):
        print(f"\n  ⚠  {target_day} is not a trading day — nothing to repair.\n")
        return

    print(f"\n  ══ Targeted Repair — {target_day} ══")
    total_inserted = 0

    for tf in timeframes:
        col = _get_col(tf)
        for symbol in symbols:
            result = _sync_symbol(fyers, symbol, tf, target_day, target_day,
                                  force_days=[target_day])
            if result is None:
                print(f"  Token expired — refreshing…")
                fyers = get_fyers()
                result = _sync_symbol(fyers, symbol, tf, target_day, target_day,
                                      force_days=[target_day])
            if result:
                sym, inserted = result
                total_inserted += inserted
                print(f"  [{sym}|{tf}m] → {inserted} inserted")

    print(f"\n  Total inserted: {total_inserted}")
    print(f"  ✅ Targeted repair complete.\n")


# ─────────────────────────────────────────────────────────────────────────────
# Clean reload
# ─────────────────────────────────────────────────────────────────────────────

def _clean_reload(
    fyers,
    symbols:    list[str],
    timeframes: list[str],
    months:     int,
) -> None:
    """
    Drop ALL existing candle data for the given symbols/TFs and reload from scratch.
    """
    print(f"\n  ══ CLEAN RELOAD — dropping all existing data ══")
    for tf in timeframes:
        col = _get_col(tf)
        for symbol in symbols:
            result = col.delete_many({"symbol": symbol})
            print(f"  Deleted {result.deleted_count:>6} docs  [{symbol}|{tf}m]")

    print(f"\n  Starting fresh preload…")
    expired = _do_preload(fyers, symbols, timeframes, months)
    if expired:
        fyers = get_fyers()
        _do_preload(fyers, symbols, timeframes, months)
    print(f"\n  ✅ Clean reload complete.\n")


# ─────────────────────────────────────────────────────────────────────────────
# Core preload (normal incremental run)
# ─────────────────────────────────────────────────────────────────────────────

def _do_preload(
    fyers,
    symbols:    list[str],
    timeframes: list[str],
    months:     int,
) -> bool:
    """
    Batch-incremental preload across all (symbol, tf) pairs.
    Returns True if a token expiry was detected.
    """
    today        = get_last_trading_day(datetime.now(IST))
    window_start = _initial_window_start(today, months)

    total   = len(symbols) * len(timeframes)
    done    = 0
    summary: dict[str, int] = {tf: 0 for tf in timeframes}

    for tf in timeframes:
        num_batches = (len(symbols) + PRELOAD_BATCH_SIZE - 1) // PRELOAD_BATCH_SIZE
        print(f"\n  ── TF {tf}m ── ({len(symbols)} symbols, {num_batches} batches)")
        print(f"     Collection  : {_col_name(tf)}")
        print(f"     Expected    : {expected_candles_per_day(int(tf))} candles/trading-day")

        col = _get_col(tf)
        sample_newest = _newest_stored_date(col, symbols[0]) if symbols else None
        if sample_newest is None:
            print(f"     Fetch mode  : INITIAL — full window {window_start} → {today}  ({months} months)")
        elif sample_newest > today:
            print(f"     Fetch mode  : UP TO DATE — newest stored: {sample_newest}")
        else:
            print(f"     Fetch mode  : INCREMENTAL — from {sample_newest} → {today}")

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
                        return True
                    sym, inserted = result
                    summary[tf] += inserted
                    done += 1
                    pct = done * 100 // total
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    sys.stdout.write(
                        f"\r  [{bar}] {done}/{total} ({pct}%)  "
                        f"batch {batch_num}/{num_batches}  "
                        f"+{inserted} inserted  {sym[:22]:<22}"
                    )
                    sys.stdout.flush()

            if batch_num < num_batches:
                time.sleep(PRELOAD_BATCH_PAUSE)

    sys.stdout.write("\n")
    print(f"\n  Preload summary (inserted = newly added candles):")
    for tf in timeframes:
        print(f"    TF {tf:>3}m — {summary[tf]:>6} inserted   →  {_col_name(tf)}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_historical_preload(
    symbols:      list[str] | None = None,
    timeframes:   list[str] | None = None,
    months:       int | None       = None,
    validate:     bool             = False,
    clean_reload: bool             = False,
    repair_date:  date | None      = None,
) -> None:
    if symbols    is None: symbols    = load_symbols()
    if timeframes is None: timeframes = HISTORICAL_TIMEFRAMES
    if months     is None: months     = HISTORICAL_PRELOAD_MONTHS

    fyers = get_fyers()

    today        = get_last_trading_day(datetime.now(IST))
    window_start = _initial_window_start(today, months)

    print(f"\n  ═══ Historical Preload — IST Naive Pipeline ═══")
    print(f"  Symbols          : {len(symbols)}")
    print(f"  Timeframes       : {timeframes}")
    print(f"  Storage format   : IST naive (datetime has no tzinfo, value is IST)")
    print(f"  Timezone policy  : fetch IST → store IST naive → read + localize IST")
    print(f"  Batch size       : {PRELOAD_BATCH_SIZE}  |  Batch pause: {PRELOAD_BATCH_PAUSE}s")
    print(f"  Mongo DB         : {_MONGO_DB}")
    print(f"  Total tasks      : {len(symbols) * len(timeframes)}")
    print()

    if repair_date is not None:
        _repair_single_date(fyers, symbols, timeframes, repair_date)
        return

    if clean_reload:
        _clean_reload(fyers, symbols, timeframes, months)
        return

    expired = _do_preload(fyers, symbols, timeframes, months)
    if expired:
        print("\n  Token expired — refreshing and retrying…")
        fyers = get_fyers()
        _do_preload(fyers, symbols, timeframes, months)

    if validate:
        _validate_and_fix(fyers, symbols, timeframes, months, verbose=True)

    print("\n  ✅ Historical preload complete.\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EMA9 Wave — IST Naive Historical Preloader")
    parser.add_argument("--symbol",       type=str,  default=None,
                        help="Single symbol, e.g. NSE:RELIANCE-EQ")
    parser.add_argument("--tf",           type=str,  default=None,
                        help="Single timeframe, e.g. 15")
    parser.add_argument("--months",       type=int,  default=None,
                        help="Override HISTORICAL_PRELOAD_MONTHS")
    parser.add_argument("--validate",     action="store_true",
                        help="Deep validate every historical day and repair broken ones")
    parser.add_argument("--clean-reload", action="store_true",
                        help="Drop all data and reload from scratch")
    parser.add_argument("--repair-date",  type=str,  default=None,
                        help="Force repair one specific IST date, e.g. 2026-05-12")
    args = parser.parse_args()

    repair_day = None
    if args.repair_date:
        try:
            repair_day = date.fromisoformat(args.repair_date)
        except ValueError:
            print(f"\n  ❌ Invalid date format: {args.repair_date}. Use YYYY-MM-DD.\n")
            sys.exit(1)

    run_historical_preload(
        symbols      = [args.symbol] if args.symbol else None,
        timeframes   = [args.tf]     if args.tf     else None,
        months       = args.months,
        validate     = args.validate,
        clean_reload = args.clean_reload,
        repair_date  = repair_day,
    )