# ================= STRATEGY — EMA 9 WAVE (−0.236 Level) =================
#
# Signal Engine — Hybrid Historical + Live
#
# Data pipeline per symbol:
#   1. Pull last HISTORY_LOOKBACK (50) closed candles from MongoDB
#      (populated by historical_preload.py)
#   2. Append live candles received from the Fyers WebSocket feed
#   3. Recalculate EMA indicators on the merged series
#   4. Run the wave state-machine bar-by-bar
#
# This gives:
#   • Smooth EMA at the live boundary (no cold-start lag)
#   • No polling delay — signals fire the moment a live candle closes
#   • Consistent results whether running backtest or live
#
# State machine (unchanged from original):
#
# IDLE → WAIT_P1 → WAIT_P3 → WAIT_P4 → WAIT_ENTRY → DONE
#
# =========================================================================

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from pymongo import MongoClient, ASCENDING

from config import (
    IST,
    ENABLE_ENTRY,
    ENTRY_MAX_CANDLES,
    HISTORY_LOOKBACK,        # NEW: 50  (candles from MongoDB for warm-up)
    MONGO_URI,
    MONGO_DB,
    HISTORICAL_TIMEFRAMES,
    TIMEFRAME,
)
from indicators import calculate_indicators, get_fib_extension_price

# ── States ────────────────────────────────────────────────────────────────────
IDLE       = "IDLE"
WAIT_P1    = "WAIT_P1"
WAIT_P3    = "WAIT_P3"
WAIT_P4    = "WAIT_P4"
WAIT_ENTRY = "WAIT_ENTRY"
DONE       = "DONE"


# ── Wave state dataclass ──────────────────────────────────────────────────────

@dataclass
class WaveState:
    """Mutable state held per symbol during a scan cycle."""
    state: str = IDLE

    p0_idx: int = -1;  p0_low: float  = 0.0; p0_dt: str = ""
    p1_idx: int = -1;  p1_high: float = 0.0; p1_dt: str = ""
    p2_idx: int = -1;  p2_low: float  = 0.0; p2_dt: str = ""
    p3_idx: int = -1;  p3_high: float = 0.0; p3_dt: str = ""
    p4_idx: int = -1;  p4_low: float  = 0.0; p4_dt: str = ""
    fib_ext_price: float = 0.0
    entry_candle_no: int = 0

    def reset(self):
        self.__init__()

    def reset_to_p0(self, idx: int, low: float, dt: str):
        self.reset()
        self.state   = WAIT_P1
        self.p0_idx  = idx
        self.p0_low  = low
        self.p0_dt   = dt
        self.p1_high = low
        self.p1_dt   = dt


# ── Live candle store (per symbol, per TF) ───────────────────────────────────

class LiveCandleStore:
    """
    Thread-safe buffer of live WebSocket candles.
    The WebSocket handler calls `push(symbol, candle_dict)`.
    The strategy engine calls `get_and_reset(symbol)` to drain the buffer.
    """

    def __init__(self):
        self._lock   = threading.Lock()
        # { symbol: [candle_dict, ...] }
        self._buffer: dict[str, list[dict]] = defaultdict(list)

    def push(self, symbol: str, candle: dict) -> None:
        """Called from WebSocket on_message callback."""
        with self._lock:
            # Merge in-progress candle: replace last if same timestamp
            buf = self._buffer[symbol]
            if buf and buf[-1]["datetime"] == candle["datetime"]:
                buf[-1] = candle
            else:
                buf.append(candle)

    def snapshot(self, symbol: str) -> list[dict]:
        """Return a copy of current buffer without clearing."""
        with self._lock:
            return list(self._buffer.get(symbol, []))

    def get_and_reset(self, symbol: str) -> list[dict]:
        """Drain and return the accumulated live candles."""
        with self._lock:
            candles = self._buffer.pop(symbol, [])
        return candles


# Module-level singleton — imported by the WebSocket client and strategy
live_store = LiveCandleStore()


# ── MongoDB helpers ───────────────────────────────────────────────────────────

def _get_collection(tf: str):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[f"candles_{tf}"]


def load_history(symbol: str, tf: str, n: int = HISTORY_LOOKBACK) -> pd.DataFrame:
    """
    Pull the last `n` closed candles for (symbol, tf) from MongoDB.
    Returns an empty DataFrame if the collection has no data yet.
    """
    col = _get_collection(tf)
    docs = list(
        col.find(
            {"symbol": symbol},
            {"_id": 0, "symbol": 0},
            sort=[("datetime", -1)],
        ).limit(n)
    )
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs[::-1])  # chronological order
    # MongoDB stores naive UTC; convert to IST
    df["datetime"] = (
        pd.to_datetime(df["datetime"], utc=True)
        .dt.tz_convert(IST)
    )
    return df


# ── Merge historical + live ───────────────────────────────────────────────────

def _live_candles_to_df(live: list[dict]) -> pd.DataFrame:
    if not live:
        return pd.DataFrame()
    df = pd.DataFrame(live)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(IST)
    return df


def build_merged_df(symbol: str, tf: str, live_candles: list[dict]) -> pd.DataFrame | None:
    """
    Merge last HISTORY_LOOKBACK historical candles with any new live candles.
    Recalculate indicators on the full merged series.
    Returns None if there is insufficient data.
    """
    hist_df = load_history(symbol, tf, n=HISTORY_LOOKBACK)
    live_df = _live_candles_to_df(live_candles)

    if hist_df.empty and live_df.empty:
        return None

    if hist_df.empty:
        merged = live_df
    elif live_df.empty:
        merged = hist_df
    else:
        merged = pd.concat([hist_df, live_df], ignore_index=True)

    # Drop dupes (live candle might overlap last stored historical candle)
    merged = (
        merged
        .drop_duplicates(subset=["datetime"])
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    # Need at least EMA_PERIOD bars
    from config import EMA_PERIOD
    if len(merged) < EMA_PERIOD:
        return None

    return calculate_indicators(merged)


# ── Utility helpers ───────────────────────────────────────────────────────────

def _dt_str(ts) -> str:
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _candle_color(row) -> str:
    return "Green" if row["close"] >= row["open"] else "Red"


# ── Signal builder ────────────────────────────────────────────────────────────

def _build_signal(symbol: str, ws: WaveState, entry_candle: Optional[dict]) -> dict:
    return {
        "symbol":        symbol,
        "p0_dt":         ws.p0_dt,   "p0_val": ws.p0_low,
        "p1_dt":         ws.p1_dt,   "p1_val": ws.p1_high,
        "p2_dt":         ws.p2_dt,   "p2_val": ws.p2_low,
        "p3_dt":         ws.p3_dt,   "p3_val": ws.p3_high,
        "p4_dt":         ws.p4_dt,   "p4_val": ws.p4_low,
        "fib_ext_price": ws.fib_ext_price,
        "entry_candle":  entry_candle,
    }


# ── Core scanner (bar-by-bar state machine) ───────────────────────────────────

def scan_symbol(
    symbol:      str,
    df:          pd.DataFrame,
    target_date: date,
) -> list[dict]:
    """
    Run the wave state machine on today's candles within `df`.

    `df` is the full merged DataFrame (historical warm-up + live candles)
    with columns: datetime, open, high, low, close, ema9_low, ema9_high.

    Only bars where datetime.date == target_date are evaluated for signals;
    prior bars exist solely to warm up the EMA.
    """
    today_df = df[df["datetime"].dt.date == target_date].reset_index(drop=True)
    if today_df.empty:
        return []

    signals: list[dict] = []
    ws = WaveState()

    for i, row in today_df.iterrows():
        high     = row["high"]
        low      = row["low"]
        close    = row["close"]
        ema_low  = row["ema9_low"]
        ema_high = row["ema9_high"]
        dt       = _dt_str(row["datetime"])

        if pd.isna(ema_low) or pd.isna(ema_high):
            continue

        # ── DONE ─────────────────────────────────────────────────────────────
        if ws.state == DONE:
            break

        # ── WAIT_ENTRY ───────────────────────────────────────────────────────
        if ws.state == WAIT_ENTRY:
            ws.entry_candle_no += 1
            color = _candle_color(row)
            if color == "Green" and close > ema_low:
                sig = _build_signal(symbol, ws, {
                    "candle_no": ws.entry_candle_no,
                    "color":     color,
                    "datetime":  dt,
                })
                signals.append(sig)
                ws.state = DONE
            elif ws.entry_candle_no >= ENTRY_MAX_CANDLES:
                ws.reset()
            continue

        # ── WAIT_P4 ──────────────────────────────────────────────────────────
        if ws.state == WAIT_P4:
            if low < ema_low and low > ws.p2_low:
                ws.p4_idx = i;  ws.p4_low = low;  ws.p4_dt = dt
                if ENABLE_ENTRY:
                    ws.state = WAIT_ENTRY
                    ws.entry_candle_no = 0
                else:
                    sig = _build_signal(symbol, ws, entry_candle=None)
                    signals.append(sig)
                    ws.state = DONE
            continue

        # ── WAIT_P3 ──────────────────────────────────────────────────────────
        if ws.state == WAIT_P3:
            if (
                high > ema_high
                and high > ws.p1_high
                and high > ws.fib_ext_price
            ):
                ws.p3_idx = i;  ws.p3_high = high;  ws.p3_dt = dt
                ws.state  = WAIT_P4
            continue

        # ── WAIT_P1 ──────────────────────────────────────────────────────────
        if ws.state == WAIT_P1:
            if high > ws.p1_high:
                ws.p1_idx  = i
                ws.p1_high = high
                ws.p1_dt   = dt
                ws.fib_ext_price = get_fib_extension_price(ws.p0_low, ws.p1_high)

            if low < ema_low:
                if low > ws.p0_low:
                    ws.p2_idx = i;  ws.p2_low = low;  ws.p2_dt = dt
                    ws.state  = WAIT_P3
                else:
                    ws.reset_to_p0(i, low, dt)
            continue

        # ── IDLE ──────────────────────────────────────────────────────────────
        if ws.state == IDLE:
            if low < ema_low:
                ws.state   = WAIT_P1
                ws.p0_idx  = i;  ws.p0_low = low;  ws.p0_dt = dt
                ws.p1_high = low;  ws.p1_dt = dt

    return signals


# ── Convenience: scan a symbol using merged data ──────────────────────────────

def scan_symbol_live(
    symbol:      str,
    tf:          str,
    target_date: date,
) -> list[dict]:
    """
    High-level call used by the live scanner loop:
      1. Drain live candles from the LiveCandleStore
      2. Merge with historical warm-up candles from MongoDB
      3. Run scan_symbol on the merged DataFrame
    """
    live_candles = live_store.snapshot(symbol)   # non-destructive read
    df = build_merged_df(symbol, tf, live_candles)
    if df is None:
        return []
    return scan_symbol(symbol, df, target_date)