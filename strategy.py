# ================= STRATEGY — EMA 9 WAVE (−0.236 Level) =================
#
# Implements CASE 1: 0.236 Validation Setup
#
# State machine per symbol (runs bar-by-bar on today's candles):
#
#   IDLE
#     └─ bar.low < ema9_low  →  record Point(0), go to WAIT_P1
#
#   WAIT_P1
#     └─ bar.high > prev_high  →  update Point(1) candidate (highest high seen)
#     └─ bar.low < ema9_low    →  price dips below EMA zone again
#          ├─ low > p0_low     →  record Point(2), go to WAIT_P3
#          └─ low <= p0_low    →  RESET: new Point(0) = this bar, stay in WAIT_P1
#
#   WAIT_P3
#     └─ bar.high > ema9_high
#        AND bar.high > p1_high
#        AND bar.high > fib_ext(-0.236)  →  record Point(3), go to WAIT_P4
#
#   WAIT_P4
#     └─ bar.low < ema9_low
#        AND bar.low > p2_low  →  record Point(4), go to WAIT_ENTRY (or SIGNAL)
#
#   WAIT_ENTRY  (only if ENABLE_ENTRY = True)
#     └─ within next ENTRY_MAX_CANDLES candles:
#          green candle AND bar.close > ema9_low  →  SIGNAL, go to DONE
#     └─ ENTRY_MAX_CANDLES elapsed with no entry  →  setup EXPIRED, go to IDLE
#
#   DONE  →  signal already emitted; suppress duplicates for this symbol
#
# Returns a signal dict or None per bar scan.
# =========================================================================

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from config import ENABLE_ENTRY, ENTRY_MAX_CANDLES
from indicators import calculate_indicators, get_fib_extension_price


# ── States ────────────────────────────────────────────────────────────────────
IDLE        = "IDLE"
WAIT_P1     = "WAIT_P1"
WAIT_P3     = "WAIT_P3"
WAIT_P4     = "WAIT_P4"
WAIT_ENTRY  = "WAIT_ENTRY"
DONE        = "DONE"


@dataclass
class WaveState:
    """Mutable state held per symbol during a scan cycle."""
    state: str = IDLE

    # Wave points
    p0_idx: int   = -1;  p0_low:  float = 0.0;  p0_dt: str = ""
    p1_idx: int   = -1;  p1_high: float = 0.0;  p1_dt: str = ""
    p2_idx: int   = -1;  p2_low:  float = 0.0;  p2_dt: str = ""
    p3_idx: int   = -1;  p3_high: float = 0.0;  p3_dt: str = ""
    p4_idx: int   = -1;  p4_low:  float = 0.0;  p4_dt: str = ""

    fib_ext_price:   float = 0.0
    entry_candle_no: int   = 0   # candles elapsed since Point(4)

    def reset(self):
        self.__init__()

    def reset_to_p0(self, idx: int, low: float, dt: str):
        """Full reset but immediately set new Point(0)."""
        self.reset()
        self.state  = WAIT_P1
        self.p0_idx = idx
        self.p0_low = low
        self.p0_dt  = dt
        self.p1_high = low   # will be updated upward
        self.p1_dt   = dt


def _dt_str(ts) -> str:
    """Format a pandas Timestamp to 'YYYY-MM-DD HH:MM'."""
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _candle_color(row) -> str:
    return "Green" if row["close"] >= row["open"] else "Red"


def scan_symbol(
    symbol: str,
    df: pd.DataFrame,
    target_date: date,
) -> list[dict]:
    """
    Run the bar-by-bar wave state machine over today's candles.

    Parameters
    ----------
    symbol      : Fyers symbol string
    df          : DataFrame with columns [datetime, open, high, low, close,
                  ema9_low, ema9_high] — already filtered to <= last_closed
    target_date : date to filter today's candles from df

    Returns
    -------
    List of signal dicts (usually 0 or 1 per day per symbol).
    Each signal dict contains all Point data + optional entry candle.
    """
    # Filter today only
    today_df = df[df["datetime"].dt.date == target_date].reset_index(drop=True)
    if today_df.empty:
        return []

    signals: list[dict] = []
    ws = WaveState()

    for i, row in today_df.iterrows():
        high      = row["high"]
        low       = row["low"]
        close     = row["close"]
        ema_low   = row["ema9_low"]
        ema_high  = row["ema9_high"]
        dt        = _dt_str(row["datetime"])

        # Skip bars where EMA hasn't warmed up yet
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
                # Valid entry candle found
                sig = _build_signal(symbol, ws, {
                    "candle_no": ws.entry_candle_no,
                    "color":     color,
                    "datetime":  dt,
                })
                signals.append(sig)
                ws.state = DONE
            elif ws.entry_candle_no >= ENTRY_MAX_CANDLES:
                # Entry window expired — reset
                ws.reset()
            continue

        # ── WAIT_P4 ──────────────────────────────────────────────────────────
        if ws.state == WAIT_P4:
            if low < ema_low and low > ws.p2_low:
                # Point(4) confirmed: lower low vs P3 but above P2 → second higher low
                ws.p4_idx  = i
                ws.p4_low  = low
                ws.p4_dt   = dt
                if ENABLE_ENTRY:
                    ws.state             = WAIT_ENTRY
                    ws.entry_candle_no   = 0
                else:
                    # Signal immediately at Point(4) formation
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
                # Point(3) confirmed
                ws.p3_idx  = i
                ws.p3_high = high
                ws.p3_dt   = dt
                ws.state   = WAIT_P4
            continue

        # ── WAIT_P1 ──────────────────────────────────────────────────────────
        if ws.state == WAIT_P1:
            # Track highest high seen after P0
            if high > ws.p1_high:
                ws.p1_idx  = i
                ws.p1_high = high
                ws.p1_dt   = dt
                # Recompute fib extension whenever P1 updates
                ws.fib_ext_price = get_fib_extension_price(ws.p0_low, ws.p1_high)

            # Check if price dips below EMA Low again
            if low < ema_low:
                if low > ws.p0_low:
                    # Valid Point(2): higher low than P0
                    ws.p2_idx = i
                    ws.p2_low = low
                    ws.p2_dt  = dt
                    ws.state  = WAIT_P3
                else:
                    # Reset: P2 went below P0 → P2 becomes new P0
                    ws.reset_to_p0(i, low, dt)
            continue

        # ── IDLE ──────────────────────────────────────────────────────────────
        if ws.state == IDLE:
            if low < ema_low:
                ws.state   = WAIT_P1
                ws.p0_idx  = i
                ws.p0_low  = low
                ws.p0_dt   = dt
                ws.p1_high = low   # seed — will be updated upward
                ws.p1_dt   = dt

    return signals


def _build_signal(symbol: str, ws: WaveState, entry_candle: Optional[dict]) -> dict:
    """Assemble the final signal dict from wave state."""
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