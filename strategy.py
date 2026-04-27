# ================= STRATEGY — EMA 9 WAVE (−0.236 Level) =================
#
# Implements CASE 1: 0.236 Validation Setup
#
# State machine per symbol (runs bar-by-bar on today's candles):
#
#   IDLE
#     └─ bar.low < ema9_low  →  start tracking Point(0) leg (lowest low),
#                               go to TRACK_P0
#
#   TRACK_P0   [was missing — added to find TRUE lowest low]
#     └─ bar.low < ema9_low  →  keep updating P0 (still going lower)
#     └─ bar.high > ema9_high →  P0 leg over: finalise lowest low seen,
#                                go to TRACK_P1
#
#   TRACK_P1   [find TRUE highest high after P0]
#     └─ bar.high > ema9_high →  keep updating P1 (still going higher)
#     └─ bar.low < ema9_low   →  P1 leg over: finalise highest high seen
#          ├─ low > p0_low    →  P2 candidate found, go to TRACK_P2
#          └─ low <= p0_low   →  invalid (new lower low) → reset:
#                                this becomes new P0, go to TRACK_P0
#
#   TRACK_P2   [find TRUE lowest low of this dip — must be > P0]
#     └─ bar.low < ema9_low  →  keep updating P2 (still going lower)
#          └─ low <= p0_low  →  P2 breached P0 → RESET: new P0 = lowest
#                               low seen so far in this leg, go to TRACK_P0
#     └─ bar.high > ema9_high →  P2 leg over: finalise P2, go to WAIT_P3
#
#   WAIT_P3
#     └─ bar.high > ema9_high
#        AND bar.high > p1_high
#        AND bar.high > fib_ext(-0.236)  →  record Point(3), go to TRACK_P4
#
#   TRACK_P4   [find TRUE lowest low of P4 leg — must stay above P2]
#     └─ bar.low < ema9_low
#          ├─ low > p2_low    →  update P4 (still going lower, still valid)
#          ├─ low <= p2_low   →  P4 broke P2 → setup INVALID, full RESET
#               └─ low <= p0_low → additionally: new P0 = this bar, TRACK_P0
#                  low >  p0_low → complete reset to IDLE (P2 broken, no reuse)
#     └─ bar.high > ema9_high →  P4 leg finalised (reversal confirmed)
#                                go to WAIT_ENTRY / emit signal
#
#   WAIT_ENTRY  (only if ENABLE_ENTRY = True)
#     └─ within next ENTRY_MAX_CANDLES candles:
#          green candle AND bar.close > ema9_low  →  SIGNAL, go to DONE
#     └─ ENTRY_MAX_CANDLES elapsed with no entry  →  setup EXPIRED, go to IDLE
#
#   DONE  →  signal already emitted; suppress duplicates for this symbol
#
# KEY FIXES vs previous versions
# ─────────────────────────────────
# Fix 1 (prior session): P0/P2 were stamped on first breaking candle.
#   → Added TRACK_P0 / TRACK_P2 states that keep updating until reversal.
#
# Fix 2 (this session): WAIT_P4 had the same first-break bug, plus no
#   invalidation path when price fell through P2 or P0.
#   → Replaced WAIT_P4 with TRACK_P4:
#       • Keeps lowering P4 while price stays below ema_low AND above p2_low
#       • If low <= p2_low  → setup invalid; full RESET to IDLE
#       • If low <= p0_low  → additionally seed new P0 from that low (TRACK_P0)
#       • Finalised only when high > ema_high (reversal confirmed)
#
# Points may span MULTIPLE candles (5, 10, 30 …) — all fine.
# Points can originate from the PREVIOUS day as long as the signal
# candle (entry candle or P4 bar) is from the current run-day.
# =========================================================================

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from config import ENABLE_ENTRY, ENTRY_MAX_CANDLES
from indicators import calculate_indicators, get_fib_extension_price


# ── States ────────────────────────────────────────────────────────────────────
IDLE       = "IDLE"
TRACK_P0   = "TRACK_P0"   # tracking lowest low for P0
TRACK_P1   = "TRACK_P1"   # tracking highest high for P1
TRACK_P2   = "TRACK_P2"   # tracking lowest low for P2
WAIT_P3    = "WAIT_P3"
TRACK_P4   = "TRACK_P4"   # tracking lowest low for P4 (must stay above P2)
WAIT_ENTRY = "WAIT_ENTRY"
DONE       = "DONE"


@dataclass
class WaveState:
    """Mutable state held per symbol during a scan cycle."""
    state: str = IDLE

    # Wave points
    p0_idx: int   = -1;  p0_low:  float = float("inf");  p0_dt: str = ""
    p1_idx: int   = -1;  p1_high: float = 0.0;           p1_dt: str = ""
    p2_idx: int   = -1;  p2_low:  float = float("inf");  p2_dt: str = ""
    p3_idx: int   = -1;  p3_high: float = 0.0;           p3_dt: str = ""
    p4_idx: int   = -1;  p4_low:  float = float("inf");  p4_dt: str = ""

    fib_ext_price:   float = 0.0
    entry_candle_no: int   = 0

    def reset(self):
        self.__init__()

    def reset_to_track_p0(self, idx: int, low: float, dt: str):
        """Full reset — immediately begin tracking a new P0 leg."""
        self.reset()
        self.state  = TRACK_P0
        self.p0_idx = idx
        self.p0_low = low
        self.p0_dt  = dt


def _dt_str(ts) -> str:
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
    Run the bar-by-bar wave state machine.

    Parameters
    ----------
    symbol      : Fyers symbol string
    df          : DataFrame with columns [datetime, open, high, low, close,
                  ema9_low, ema9_high].
                  May include previous day candles — the state machine will
                  run across all of them.  Only the signal candle (entry or P4)
                  must fall on target_date.
    target_date : date that the SIGNAL candle must belong to.

    Returns
    -------
    List of signal dicts (usually 0 or 1 per day per symbol).
    """
    # Run state machine over ALL bars in df (may include prior day).
    # We validate target_date only at signal-emit time.
    if df.empty:
        return []

    signals: list[dict] = []
    ws = WaveState()

    for i, row in df.iterrows():
        high     = row["high"]
        low      = row["low"]
        close    = row["close"]
        ema_low  = row["ema9_low"]
        ema_high = row["ema9_high"]
        dt       = _dt_str(row["datetime"])
        bar_date = row["datetime"].date()

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
                # Signal candle must be from today
                if bar_date == target_date:
                    sig = _build_signal(symbol, ws, {
                        "candle_no": ws.entry_candle_no,
                        "color":     color,
                        "datetime":  dt,
                    })
                    signals.append(sig)
                    ws.state = DONE
                # If not today yet — keep waiting; window still open
            elif ws.entry_candle_no >= ENTRY_MAX_CANDLES:
                ws.reset()
            continue

        # ── TRACK_P4 ─────────────────────────────────────────────────────────
        # Entered when P3 is confirmed and price first dips below ema_low.
        # Keep updating P4 (lowest low) while price stays below ema_low
        # AND remains above P2.  Finalise when price reverses above ema_high.
        #
        # Invalidation rules:
        #   low <= p2_low  → P4 broke through P2 → entire setup invalid
        #       low <= p0_low → that extreme becomes new P0; restart TRACK_P0
        #       low >  p0_low → full reset to IDLE (no reusable anchor)
        if ws.state == TRACK_P4:
            if low < ema_low:
                if low <= ws.p2_low:
                    # ── Setup invalid: P4 fell through P2 ────────────────
                    if low <= ws.p0_low:
                        # Also broke P0 → seed new P0 from this bar
                        ws.reset_to_track_p0(i, low, dt)
                    else:
                        # Between P0 and P2 — no reusable structure, full IDLE
                        ws.reset()
                else:
                    # Still valid — update P4 running minimum
                    if low < ws.p4_low:
                        ws.p4_idx = i
                        ws.p4_low = low
                        ws.p4_dt  = dt

            # Check AFTER possible state change (reset may have changed state)
            if ws.state == TRACK_P4 and high > ema_high:
                # Price reversed above EMA high — P4 leg finalised
                if ENABLE_ENTRY:
                    ws.state           = WAIT_ENTRY
                    ws.entry_candle_no = 0
                else:
                    if bar_date == target_date:
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
                ws.p3_idx  = i
                ws.p3_high = high
                ws.p3_dt   = dt
                ws.state   = TRACK_P4
                # Reset P4 tracker (inf signals "not yet touched ema_low")
                ws.p4_idx = -1
                ws.p4_low = float("inf")
                ws.p4_dt  = ""
            continue

        # ── TRACK_P2 ─────────────────────────────────────────────────────────
        # We are in a downward leg after P1. Keep lowering P2 candidate until
        # price reverses back above ema_high.
        if ws.state == TRACK_P2:
            if low < ema_low:
                if low <= ws.p0_low:
                    # P2 has broken below P0 — invalid setup.
                    # The lowest low seen in THIS leg becomes the new P0.
                    # (ws.p2_low is already the running minimum of this leg)
                    new_p0_low = min(ws.p2_low, low)
                    new_p0_dt  = dt if low <= ws.p2_low else ws.p2_dt
                    ws.reset_to_track_p0(i, new_p0_low, new_p0_dt)
                else:
                    # Still valid — update P2 if we see a lower low
                    if low < ws.p2_low:
                        ws.p2_idx = i
                        ws.p2_low = low
                        ws.p2_dt  = dt

            if ws.state == TRACK_P2 and high > ema_high:
                # Price reversed back above EMA high — P2 leg is finalised.
                # P2 is the lowest low recorded during this leg.
                ws.state = WAIT_P3
            continue

        # ── TRACK_P1 ─────────────────────────────────────────────────────────
        # We are in an upward leg after P0. Keep raising P1 candidate until
        # price reverses back below ema_low.
        if ws.state == TRACK_P1:
            if high > ema_high:
                # Still in upward leg — update P1 if new high
                if high > ws.p1_high:
                    ws.p1_idx  = i
                    ws.p1_high = high
                    ws.p1_dt   = dt
                    ws.fib_ext_price = get_fib_extension_price(
                        ws.p0_low, ws.p1_high
                    )

            if low < ema_low:
                # Price reversed below EMA low — P1 is finalised.
                # This bar is the FIRST bar of the P2 leg.
                if low > ws.p0_low:
                    # Valid P2 candidate: higher low than P0
                    ws.p2_idx = i
                    ws.p2_low = low
                    ws.p2_dt  = dt
                    ws.state  = TRACK_P2
                else:
                    # Low broke below P0 — full reset, this low seeds new P0
                    ws.reset_to_track_p0(i, low, dt)
            continue

        # ── TRACK_P0 ─────────────────────────────────────────────────────────
        # We are in the initial downward leg. Keep lowering P0 candidate until
        # price reverses back above ema_high.
        if ws.state == TRACK_P0:
            if low < ema_low:
                # Still pushing lower — update P0
                if low < ws.p0_low:
                    ws.p0_idx = i
                    ws.p0_low = low
                    ws.p0_dt  = dt

            if high > ema_high:
                # Price reversed above EMA high — P0 leg finalised.
                # Seed P1 tracking: start from this bar.
                ws.p1_idx  = i
                ws.p1_high = high
                ws.p1_dt   = dt
                ws.fib_ext_price = get_fib_extension_price(
                    ws.p0_low, ws.p1_high
                )
                ws.state = TRACK_P1
            continue

        # ── IDLE ──────────────────────────────────────────────────────────────
        if ws.state == IDLE:
            if low < ema_low:
                ws.state  = TRACK_P0
                ws.p0_idx = i
                ws.p0_low = low
                ws.p0_dt  = dt

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