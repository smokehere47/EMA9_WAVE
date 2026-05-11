# ================= STRATEGY — EMA 9 WAVE  (Mother Wave Identification) =======
#
# TIMEZONE POLICY:
# ─────────────────
#   MongoDB stores IST NAIVE datetimes (no tzinfo, value is IST local time).
#   load_history (patched in main.py) returns IST-AWARE DataFrame
#   by doing tz_localize(IST) on the IST naive values.
#
#   _live_candles_to_df: live candles from LiveCandleStore hold datetime as
#   dt_ist.isoformat() — an IST-aware string ("+05:30" suffix).
#   We parse with pd.to_datetime() which preserves the offset, then
#   .dt.tz_convert(IST) normalises to IST-aware — matching the MongoDB data.
#
#   Both DataFrames are IST-aware after their respective conversions.
#   drop_duplicates(subset=["datetime"]) and sort_values("datetime") work
#   correctly because both columns are in the same timezone.
#
# LIVE vs DEV MODE — combined OHLC stream:
# ─────────────────────────────────────────
#   In LIVE mode the scanner passes is_live=True so live WebSocket candles
#   are merged with historical MongoDB data for every symbol.
#
#   In DEV / BACKTEST mode (OVERRIDE_TRADING_DAY or OVERRIDE_DATE_RANGE) the
#   scanner used to pass is_live=False, which meant live_store was never
#   consulted and dev-mode symbols only got historical data.
#
#   FIX: build_merged_df now accepts an explicit live_candles list rather than
#   a boolean flag. The caller is responsible for deciding whether to pass
#   live candles.  In main.py both live AND dev-mode scans now call
#   live_store.snapshot(symbol) and pass the result here.  An empty list
#   (no live candles yet) is a safe no-op — the merge logic handles it.
#
#   This means dev-mode symbols get the same combined historical+live stream
#   as live-mode symbols, which is the correct behaviour for intraday testing.
#
# WAVE NUMBERING (after all changes):
# ─────────────────────────────────────
#   Wave 1  = most recent wave (closest to run day / target_date)
#   Wave N  = oldest wave in the lookback window
#   Motherwave = largest by size among all collected waves (any number)
#
# ============================================================================

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import pandas as pd
from pymongo import MongoClient, ASCENDING

from config import (
    IST,
    MONGO_CREDS_FILE,
    MONGO_DB_DEFAULT,
)
from indicators import calculate_indicators


# ─────────────────────────────────────────────────────────────────────────────
# Wave constants & data structures
# ─────────────────────────────────────────────────────────────────────────────

WAVE_UP   = "up"
WAVE_DOWN = "down"


@dataclass
class Wave:
    """One identified wave segment (up-wave or down-wave)."""
    wave_type:  str
    wave_num:   int
    low:        float
    low_dt:     str
    high:       float
    high_dt:    str
    size:       float


@dataclass
class FractalSignal:
    """
    One confirmed Bearish Fractal inside a down-wave segment.
    Represents an Uptrend Split [F] that chops the parent down-wave.
    """
    high:        float
    high_dt:     str
    c1_dt:       str
    c3_dt:       str
    sma20:       float
    ema9_high:   float
    ema9_low:    float
    wave_low:    float
    wave_low_dt: str


# ─────────────────────────────────────────────────────────────────────────────
# Live candle store
# ─────────────────────────────────────────────────────────────────────────────

class LiveCandleStore:
    """
    Thread-safe in-memory buffer for live WebSocket candles.

    Keys: symbol (str)
    Values: list of candle dicts, with datetime stored as IST-aware isoformat string.

    Behaviour:
      push()   — append new candle or update if same datetime (same candle, new tick)
      snapshot() — read-only copy (does NOT remove from buffer)
      get_and_reset() — read + clear (used when all data is being forwarded)

    Both live and dev-mode scanners call snapshot() so they always receive
    whatever live candles are available at scan time.
    """

    def __init__(self):
        self._lock   = threading.Lock()
        self._buffer: dict[str, list[dict]] = defaultdict(list)

    def push(self, symbol: str, candle: dict) -> None:
        with self._lock:
            buf = self._buffer[symbol]
            if buf and buf[-1]["datetime"] == candle["datetime"]:
                buf[-1] = candle
            else:
                buf.append(candle)

    def snapshot(self, symbol: str) -> list[dict]:
        with self._lock:
            return list(self._buffer.get(symbol, []))

    def get_and_reset(self, symbol: str) -> list[dict]:
        with self._lock:
            return self._buffer.pop(symbol, [])

    def all_symbols(self) -> list[str]:
        """Return all symbols that currently have live candles buffered."""
        with self._lock:
            return list(self._buffer.keys())


live_store = LiveCandleStore()


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB helpers (base — patched in main.py to _adaptive_load_history)
# ─────────────────────────────────────────────────────────────────────────────

def _get_collection(tf: str):
    client = MongoClient(MONGO_CREDS_FILE)
    db     = client[MONGO_DB_DEFAULT]
    return db[f"candle_{tf}"]


def load_history(symbol: str, tf: str, n: int = 500) -> pd.DataFrame:
    """
    Base implementation — patched at runtime by main.py to _adaptive_load_history.

    Reads IST naive datetimes from MongoDB and localizes them to IST-aware,
    which is what all downstream code (calculate_indicators, wave scan) expects.
    """
    col  = _get_collection(tf)
    docs = list(
        col.find(
            {"symbol": symbol},
            {"_id": 0, "symbol": 0},
            sort=[("datetime", -1)],
        ).limit(n)
    )
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs[::-1])
    # IST naive → IST-aware (correct because stored values ARE already IST)
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Merge historical + live candles
# ─────────────────────────────────────────────────────────────────────────────

def _live_candles_to_df(live: list[dict]) -> pd.DataFrame:
    """
    Convert live candles from LiveCandleStore to IST-aware DataFrame.

    Live candles have datetime = dt_ist.isoformat() which contains the +05:30
    offset (e.g. "2026-05-07T09:15:00+05:30"). pd.to_datetime parses the offset
    correctly; .dt.tz_convert(IST) normalises to IST-aware, matching load_history.
    """
    if not live:
        return pd.DataFrame()
    df = pd.DataFrame(live)
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert(IST)
    return df


def build_merged_df(
    symbol:      str,
    tf:          str,
    live_candles: list[dict],
) -> pd.DataFrame | None:
    """
    Merge historical MongoDB candles with live WebSocket candles.

    This function is intentionally mode-agnostic.  The caller decides whether
    to pass live candles — an empty list is safe and simply means no live data
    is available yet.  This makes it work correctly for BOTH live and dev/
    backtest modes without any special-casing here.

    The merge process:
      1. Load history from MongoDB (IST-aware after tz_localize).
      2. Convert live candles to IST-aware DataFrame.
      3. Concatenate, deduplicate on datetime, sort chronologically.
      4. Drop rows until we have at least EMA_PERIOD candles.
      5. Run calculate_indicators on the merged result.

    Deduplication note:
      Live candles represent the open (incomplete) bar.  If MongoDB already
      has a completed bar for the same datetime (e.g. from a previous run that
      persisted the candle), drop_duplicates keeps the LAST occurrence, which
      is the live candle with the most recent OHLCV values.  This is the
      desired behaviour — live data wins for the current bar.
    """
    hist_df = load_history(symbol, tf)
    live_df = _live_candles_to_df(live_candles)

    if hist_df.empty and live_df.empty:
        return None

    if hist_df.empty:
        merged = live_df
    elif live_df.empty:
        merged = hist_df
    else:
        # Stack historical first so that live candles (same datetime) win
        # when drop_duplicates keeps='last'.
        merged = pd.concat([hist_df, live_df], ignore_index=True)

    merged = (
        merged
        .drop_duplicates(subset=["datetime"], keep="last")  # live wins for open bar
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    from config import EMA_PERIOD
    if len(merged) < EMA_PERIOD:
        return None

    return calculate_indicators(merged)


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def _dt_str(ts) -> str:
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _body_low(row: dict) -> float:
    return min(row["open"], row["close"])


def _is_green(row: dict) -> bool:
    return row["close"] >= row["open"]


def _active_price(row: dict) -> float:
    return row["close"] if _is_green(row) else row["open"]


def _ema9_low(row: dict) -> float | None:
    v = row.get("ema9_low")
    if v is None or (isinstance(v, float) and v != v):
        return None
    return float(v)


def _ema9_high(row: dict) -> float | None:
    v = row.get("ema9_high")
    if v is None or (isinstance(v, float) and v != v):
        return None
    return float(v)


def _sma20(row: dict) -> float | None:
    v = row.get("sma20")
    if v is None or (isinstance(v, float) and v != v):
        return None
    return float(v)


# ─────────────────────────────────────────────────────────────────────────────
# Core: identify waves (forward scan — Wave 1 = latest, Wave N = oldest)
# ─────────────────────────────────────────────────────────────────────────────

def _identify_waves_forward(
    df:          pd.DataFrame,
    target_date: date,
) -> list[Wave]:
    """
    Scan FORWARD oldest → latest collecting up and down waves.

    Wave detection:
      - A pivot LOW cluster = consecutive candles where body_low < ema9_low.
        The lowest body_low in the cluster is the pivot low.
      - Between two consecutive pivot low clusters, the highest candle
        where high > ema9_high is the wave high.
        If no candle clears ema9_high the segment is skipped (hard rule).

    Wave numbering:
      Wave 1 = most recent, Wave N = oldest (assigned after collection).

    Output: chronological list of up-waves and interleaved down-waves,
            trimmed to latest WAVE_OUTPUT_COUNT waves.
    """
    from config import WAVE_OUTPUT_COUNT

    df_scan = df[df["datetime"].dt.date <= target_date].reset_index(drop=True)
    if df_scan.empty:
        return []

    rows = df_scan.to_dict("records")
    n    = len(rows)

    # ── Mark below-EMA9-low ───────────────────────────────────────────────
    below: list[bool] = []
    for row in rows:
        ema_l = _ema9_low(row)
        below.append(False if ema_l is None else _body_low(row) < ema_l)

    # ── Build pivot low clusters (consecutive below-EMA runs) ─────────────
    clusters: list[tuple[int, int]] = []
    i = 0
    while i < n:
        if below[i]:
            j = i
            while j < n and below[j]:
                j += 1
            clusters.append((i, j - 1))
            i = j
        else:
            i += 1

    if len(clusters) < 2:
        return []

    # ── Per cluster: pivot low = candle with lowest body_low ──────────────
    pivots: list[dict] = []
    for c_start, c_end in clusters:
        best_idx  = c_start
        best_blow = _body_low(rows[c_start])
        for k in range(c_start + 1, c_end + 1):
            bl = _body_low(rows[k])
            if bl < best_blow:
                best_blow = bl
                best_idx  = k
        pivots.append({
            "cluster_start": c_start,
            "cluster_end":   c_end,
            "low":           best_blow,
            "low_dt":        _dt_str(rows[best_idx]["datetime"]),
        })

    # ── Collect up-waves forward (oldest → latest) ────────────────────────
    up_waves_raw: list[dict] = []

    for p_idx in range(len(pivots) - 1):
        p_curr = pivots[p_idx]       # left (older) pivot
        p_next = pivots[p_idx + 1]   # right (newer) pivot

        seg_start = p_curr["cluster_end"] + 1
        seg_end   = p_next["cluster_start"] - 1

        if seg_start > seg_end:
            continue

        # ── Hard rule: wave high must have high > ema9_high ───────────────
        wave_high    = None
        wave_high_dt = None

        for k in range(seg_start, seg_end + 1):
            ema_h_k = _ema9_high(rows[k])
            if ema_h_k is None:
                continue
            if rows[k]["high"] > ema_h_k:
                if wave_high is None or rows[k]["high"] > wave_high:
                    wave_high    = rows[k]["high"]
                    wave_high_dt = _dt_str(rows[k]["datetime"])

        if wave_high is None:
            # No candle cleared ema9_high — skip this segment (hard rule)
            continue

        up_waves_raw.append({
            "low":     p_curr["low"],
            "low_dt":  p_curr["low_dt"],
            "high":    wave_high,
            "high_dt": wave_high_dt,
            "size":    abs(wave_high - p_curr["low"]),
        })

    if not up_waves_raw:
        return []

    # ── Assign wave numbers: Wave 1 = newest, Wave N = oldest ─────────────
    total = len(up_waves_raw)
    all_up: list[Wave] = []
    for idx, uw in enumerate(up_waves_raw):
        wave_num = total - idx
        all_up.append(Wave(
            wave_type = WAVE_UP,
            wave_num  = wave_num,
            low       = uw["low"],
            low_dt    = uw["low_dt"],
            high      = uw["high"],
            high_dt   = uw["high_dt"],
            size      = uw["size"],
        ))

    # ── Interleave down-waves ─────────────────────────────────────────────
    result_waves: list[Wave] = []
    for idx, uw in enumerate(all_up):
        result_waves.append(uw)
        if idx + 1 < len(all_up):
            nxt = all_up[idx + 1]
            result_waves.append(Wave(
                wave_type = WAVE_DOWN,
                wave_num  = uw.wave_num,
                low       = nxt.low,
                low_dt    = nxt.low_dt,
                high      = uw.high,
                high_dt   = uw.high_dt,
                size      = abs(uw.high - nxt.low),
            ))

    # ── Rolling window: keep only latest WAVE_OUTPUT_COUNT waves ──────────
    if len(result_waves) > WAVE_OUTPUT_COUNT:
        result_waves = result_waves[-WAVE_OUTPUT_COUNT:]

    return result_waves

# ─────────────────────────────────────────────────────────────────────────────
# Mother Wave selection
# ─────────────────────────────────────────────────────────────────────────────

def _select_mother_waves(
    waves: list[Wave],
) -> tuple[Optional[Wave], Optional[Wave], Optional[Wave]]:
    if not waves:
        return None, None, None

    by_size     = sorted(waves, key=lambda w: w.size, reverse=True)
    motherwave  = by_size[0] if len(by_size) >= 1 else None
    second_wave = by_size[1] if len(by_size) >= 2 else None
    third_wave  = None

    if motherwave:
        threshold  = motherwave.size / 3.0
        candidates = [
            w for w in waves
            if w is not motherwave and w is not second_wave and w.size <= threshold
        ]
        if candidates:
            max_q = max(c.size for c in candidates)
            for w in reversed(waves):
                if w in candidates and w.size == max_q:
                    third_wave = w
                    break

    return motherwave, second_wave, third_wave


# ─────────────────────────────────────────────────────────────────────────────
# Fractal detection — Bearish Fractal inside a down-wave segment
# ─────────────────────────────────────────────────────────────────────────────

def _find_fractal_signals(
    seg_rows:    list[dict],
    target_date: date,
) -> list[FractalSignal]:
    """
    Detect Downtrend → Uptrend Split [F] fractals inside a down-wave segment.

    seg_rows is in CHRONOLOGICAL order:
      seg_rows[0]  = first candle AFTER the wave HIGH (price falling)
      seg_rows[-1] = candle AT (or near) the wave LOW

    Because the segment is a downtrend, a fractal here is a temporary upward
    spike — a local HIGH inside the falling price action.  That is exactly the
    "Downtrend → Uptrend Split [F]" pattern.  There is NO uptrend-split search
    (that would look for local lows inside an up-wave, which we do not do).

    Rules (strictly enforced):
    ──────────────────────────
    1. WICK condition  : C2.high  > C1.high  AND  C2.high  > C3.high
       (C2's wick sticks up above both neighbours — a local high)

    2. BODY condition  : C2_active > C1_active  AND  C2_active > C3_active
       where active_price = close if candle is green (close >= open)
                          = open  if candle is red   (close <  open)
       (The body of C2 also peaks above neighbours, confirming real buying
        pressure, not just a wick poke.)

    3. SMA20 condition : C2.high < SMA20_of_C2
       (The fractal high must remain BELOW the 20-period SMA of C2.
        Only C2 is checked — not C1 or C3.)

    Tie-breaking for flat tops (multiple candles share the same high):
      The first candle at that high level is used as C2; the cluster is
      extended rightward (mid_end), and C3 is the candle after the cluster.
      The SMA check uses only the first C2 in the cluster.
    """
    n = len(seg_rows)
    if n < 3:
        return []

    signals: list[FractalSignal] = []
    i = 1

    while i < n - 1:
        c2_row  = seg_rows[i]
        c2_high = c2_row["high"]

        # Handle flat-top clusters: find rightmost candle with same high
        mid_end = i
        while mid_end + 1 < n - 1 and seg_rows[mid_end + 1]["high"] == c2_high:
            mid_end += 1

        c1 = seg_rows[i - 1]
        c3 = seg_rows[mid_end + 1]

        # ── Rule 1: wick condition ────────────────────────────────────────────
        if not (c1["high"] < c2_high and c3["high"] < c2_high):
            i += 1
            continue

        # ── Rule 2: body condition — lower body of C2 above lower body of C1 and C3 ──
        c2_body_low = _body_low(c2_row)
        c1_body_low = _body_low(c1)
        c3_body_low = _body_low(c3)

        if not (c2_body_low > c1_body_low and c2_body_low > c3_body_low):
            i += 1
            continue
        
        # ── Wick low condition: C2 low must be above C1 low and C3 low ───────────
        if not (c2_row["low"] > c1["low"] and c2_row["low"] > c3["low"]):
            i += 1
            continue

        # ── Rule 3: SMA20 condition — only C2's high vs C2's own SMA20 ───────
        sma   = _sma20(c2_row)
        ema_h = _ema9_high(c2_row)
        ema_l = _ema9_low(c2_row)

        if sma is None or ema_h is None or ema_l is None:
            i = mid_end + 2
            continue

        if c2_high >= sma:
            # C2's high is AT or ABOVE SMA20 — not a valid fractal
            i = mid_end + 2
            continue

        # ── Wave low: lowest `low` where close < ema9_low (soft fallback) ────
        # Primary: find candle with lowest `low` that also closes below ema9_low.
        # Fallback: if no candle meets the close condition, use lowest `low` as-is.

        wave_low     = None
        wave_low_idx = 0

        # ── Pass 1: primary — lowest low with close < ema9_low ───────────────
        for k in range(0, i):
            row_k  = seg_rows[k]
            ema_l_k = _ema9_low(row_k)
            if ema_l_k is None:
                continue
            if row_k["close"] < ema_l_k:
                if wave_low is None or row_k["low"] < wave_low:
                    wave_low     = row_k["low"]
                    wave_low_idx = k

        # ── Pass 2: fallback — lowest low unconditionally ────────────────────
        if wave_low is None:
            wave_low     = seg_rows[0]["low"]
            wave_low_idx = 0
            for k in range(1, i):
                if seg_rows[k]["low"] < wave_low:
                    wave_low     = seg_rows[k]["low"]
                    wave_low_idx = k

        signals.append(FractalSignal(
            high        = c2_high,
            high_dt     = _dt_str(c2_row["datetime"]),
            c1_dt       = _dt_str(c1["datetime"]),
            c3_dt       = _dt_str(c3["datetime"]),
            sma20       = round(sma,      4),
            ema9_high   = round(ema_h,    4),
            ema9_low    = round(ema_l,    4),
            wave_low    = round(wave_low, 4),
            wave_low_dt = _dt_str(seg_rows[wave_low_idx]["datetime"]),
        ))

        i = mid_end + 2

    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Output formatters
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_selected_wave(label: str, w: Wave) -> str:
    wave_label = "downWave" if w.wave_type == WAVE_DOWN else "Wave"
    return (
        f"{label}:\n"
        f"  {wave_label} No. : {w.wave_num}\n"
        f"  Size: {w.size:,.2f}"
        f"   Higher High → ({w.high_dt}) ({w.high:.2f})"
        f"   Lower Low   → ({w.low_dt}) ({w.low:.2f})"
    )


def _fmt_wave_row(w: Wave) -> str:
    if w.wave_type == "up[F]":
        label = f"Wave {w.wave_num}[F]"
    elif w.wave_type == WAVE_UP:
        label = f"Wave {w.wave_num}"
    else:
        label = f"downWave {w.wave_num}"

    if w.wave_type == WAVE_DOWN:
        return (
            f"{label}:\n"
            f"  Size: {w.size:,.2f}"
            f"   From High {w.wave_num} → ({w.high_dt}) ({w.high:.2f})"
            f"   To Low {w.wave_num}    → ({w.low_dt}) ({w.low:.2f})"
        )
    return (
        f"{label}:\n"
        f"  Size: {w.size:,.2f}"
        f"   Higher High {w.wave_num} → ({w.high_dt}) ({w.high:.2f})"
        f"   Lower Low {w.wave_num}   → ({w.low_dt}) ({w.low:.2f})"
    )


def format_signal(sig: dict) -> str:
    lines: list[str] = []
    lines.append(f"Stock Name: {sig['symbol']}")

    for key, label in [
        ("_wave_motherwave",  "Motherwave"),
        ("_wave_second_wave", "2ndlargestwave"),
        ("_wave_third_wave",  "3xsmallerwave"),
    ]:
        w = sig.get(key)
        lines.append(
            _fmt_selected_wave(label, w) if w else f"{label}:\n  (not identified)"
        )

    for w in sig.get("_all_waves", []):
        lines.append(_fmt_wave_row(w))

    return "\n".join(lines)


def print_signal(sig: dict) -> None:
    print(format_signal(sig))
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Public API — scan_symbol  (called by main.py)
# ─────────────────────────────────────────────────────────────────────────────

def scan_symbol(
    symbol:      str,
    df:          pd.DataFrame,
    target_date: date,
) -> list[dict]:
    waves = _identify_waves_forward(df, target_date)
    if not waves:
        return []

    def _flat(prefix: str, w: Optional[Wave]) -> dict:
        if w is None:
            return {f"{prefix}_{k}": None for k in
                    ("type", "wave_num", "size", "low", "low_dt", "high", "high_dt")}
        return {
            f"{prefix}_type":     w.wave_type,
            f"{prefix}_wave_num": w.wave_num,
            f"{prefix}_size":     round(w.size, 4),
            f"{prefix}_low":      round(w.low,  4),
            f"{prefix}_low_dt":   w.low_dt,
            f"{prefix}_high":     round(w.high, 4),
            f"{prefix}_high_dt":  w.high_dt,
        }

    df_filtered = df[df["datetime"].dt.date <= target_date]
    df_rows     = df_filtered.to_dict("records")

    dt_to_idx: dict[str, int] = {
        _dt_str(r["datetime"]): k for k, r in enumerate(df_rows)
    }

    fractal_by_downwave: dict[str, list[FractalSignal]] = {}
    all_fractal_signals: list[FractalSignal]            = []

    for w in waves:
        if w.wave_type != WAVE_DOWN:
            continue

        high_idx = dt_to_idx.get(w.high_dt)
        low_idx  = dt_to_idx.get(w.low_dt)
        if high_idx is None or low_idx is None:
            continue

        seg_start = high_idx + 1
        seg_end   = low_idx
        if seg_start > seg_end:
            continue

        seg_rows = df_rows[seg_start : seg_end + 1]
        if len(seg_rows) < 3:
            continue

        seg_fractals = _find_fractal_signals(seg_rows, target_date)
        if seg_fractals:
            fractal_by_downwave[w.high_dt] = seg_fractals
            all_fractal_signals.extend(seg_fractals)

    up_waves_only = [w for w in waves if w.wave_type == WAVE_UP]

    pre_final: list[Wave] = []
    for uw in up_waves_only:
        pre_final.append(uw)
        for fs in fractal_by_downwave.get(uw.high_dt, []):
            pre_final.append(Wave(
                wave_type = "up[F]",
                wave_num  = 0,
                low       = fs.wave_low,
                low_dt    = fs.wave_low_dt,
                high      = fs.high,
                high_dt   = fs.high_dt,
                size      = abs(fs.high - fs.wave_low),
            ))

    total_up   = len(pre_final)
    final_waves: list[Wave] = []

    for idx, uw in enumerate(pre_final):
        wave_num = total_up - idx
        final_waves.append(Wave(
            wave_type = uw.wave_type,
            wave_num  = wave_num,
            low       = uw.low,
            low_dt    = uw.low_dt,
            high      = uw.high,
            high_dt   = uw.high_dt,
            size      = uw.size,
        ))

    all_up      = list(final_waves)
    final_waves = []

    for idx, uw in enumerate(all_up):
        final_waves.append(uw)
        if idx + 1 < len(all_up):
            nxt = all_up[idx + 1]
            final_waves.append(Wave(
                wave_type = WAVE_DOWN,
                wave_num  = uw.wave_num,
                low       = nxt.low,
                low_dt    = nxt.low_dt,
                high      = uw.high,
                high_dt   = uw.high_dt,
                size      = abs(uw.high - nxt.low),
            ))

    waves = final_waves
    motherwave, second_wave, third_wave = _select_mother_waves(waves)

    result = {
        "symbol":            symbol,
        "scan_date":         str(target_date),
        "total_waves_found": len(waves),

        **_flat("motherwave",  motherwave),
        **_flat("second_wave", second_wave),
        **_flat("third_wave",  third_wave),

        "fractal_count":        len(all_fractal_signals),
        "_fractal_signals":     all_fractal_signals,
        "_fractal_by_downwave": fractal_by_downwave,

        "_wave_motherwave":  motherwave,
        "_wave_second_wave": second_wave,
        "_wave_third_wave":  third_wave,
        "_all_waves":        waves,
    }
    return [result]


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: scan a symbol live (always merges live candles)
# ─────────────────────────────────────────────────────────────────────────────

def scan_symbol_live(
    symbol:      str,
    tf:          str,
    target_date: date,
) -> list[dict]:
    """
    Convenience wrapper: always fetches live candles from live_store.
    Works for both live and dev modes — passes live candles regardless.
    """
    live_candles = live_store.snapshot(symbol)
    df = build_merged_df(symbol, tf, live_candles)
    if df is None:
        return []
    return scan_symbol(symbol, df, target_date)