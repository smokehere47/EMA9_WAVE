# ================= STRATEGY — EMA 9 WAVE  (Mother Wave Identification) =======
#
# Signal Engine — Mother Wave Identification
#
# Overview
# ────────
# Starting from the run-day (or backtest target-date), the engine collects up
# to MOTHERWAVE_LOOKBACK (50) waves by scanning forward through historical
# candles (oldest → newest).  From those waves it labels three special waves:
#
#   Motherwave   — the LARGEST wave by size across all collected waves
#   2nd-largest  — the second largest wave by size (any type)
#   3x-smaller   — among all same-type waves (excl. motherwave & 2nd-largest)
#                  whose size ≤ motherwave.size / 3, pick the one whose size
#                  is CLOSEST TO motherwave/3 (i.e. the largest qualifying one).
#                  Break ties by recency (closest to run-day).
#
# Chronological proximity to run-day:
# ────────────────────────────────────
#   3x-smaller  →  most recent  (closest to run-day)
#   2nd-largest →  middle
#   Motherwave  →  oldest       (farthest from run-day)
#
# Wave definitions
# ────────────────
# A NORMAL WAVE (bullish swing):
#   Low  → price dips below EMA-9-low zone.
#          The effective wave-low is the LOWEST body-price (min of open, close)
#          across ALL consecutive candles whose bodies cross below ema9_low.
#          Not just the first crossing candle — the entire cluster counts.
#   High → the HIGHEST wick-high of all candles between the end of this
#          wave-low cluster and the start of the next wave-low cluster.
#   Size → abs(high − low)
#
# A COUNTER WAVE (pullback between two consecutive normal waves):
#   Spans from the HIGH of normal-wave[N] to the LOW of normal-wave[N+1].
#   Size → abs(wave[N].high − wave[N+1].low)
#
# Wave numbering (chronological)
# ───────────────────────────────
#   Normal waves  → Wave 1, Wave 2, … Wave N   (Wave 1 = oldest)
#   Counter waves → counterWave 1 follows Wave 1, counterWave 2 follows Wave 2…
#
# Output format (see format_signal())
# ────────────────────────────────────
#   Stock Name: <SYMBOL>
#   Motherwave:
#     Wave No. : x
#     Size: 342.34   Higher High → (dateTime) (High Value)   Lower Low → (dateTime) (Low Value)
#   2ndlargestwave:
#     Wave No. : x
#     Size: ...
#   3xsmallerwave:
#     Wave No. : x
#     Size: ...
#   Wave 1:
#     Size: ...   Higher High 1 → (dateTime) (High Value)   Lower Low 1 → (dateTime) (Low Value)
#   counterWave 1:
#     Size: ...
#   Wave 2:
#     ...
#
# ============================================================================

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import pandas as pd
from pymongo import MongoClient, ASCENDING

from config import (
    IST,
    HISTORY_LOOKBACK,
    MOTHERWAVE_LOOKBACK,
    MONGO_CREDS_FILE,
    MONGO_DB_DEFAULT,
)
from indicators import calculate_indicators


# ─────────────────────────────────────────────────────────────────────────────
# Wave constants & data structures
# ─────────────────────────────────────────────────────────────────────────────

WAVE_NORMAL  = "normal"    # bullish swing: pivot-low → swing-high
WAVE_COUNTER = "counter"   # pullback between two consecutive normal waves


@dataclass
class Wave:
    """One identified wave segment (normal or counter)."""
    wave_type:  str    # WAVE_NORMAL or WAVE_COUNTER
    wave_num:   int    # sequential number in chronological order (1 = oldest)
    low:        float  # Lower Low price
    low_dt:     str    # datetime string of the Lower Low candle
    high:       float  # Higher High price
    high_dt:    str    # datetime string of the Higher High candle
    size:       float  # abs(high - low)


# ─────────────────────────────────────────────────────────────────────────────
# Live candle store  (unchanged architecture — feeds WebSocket ticks)
# ─────────────────────────────────────────────────────────────────────────────

class LiveCandleStore:
    """
    Thread-safe buffer of live WebSocket candles.
    WebSocket on_message calls push(); strategy engine calls snapshot().
    """

    def __init__(self):
        self._lock   = threading.Lock()
        self._buffer: dict[str, list[dict]] = defaultdict(list)

    def push(self, symbol: str, candle: dict) -> None:
        with self._lock:
            buf = self._buffer[symbol]
            if buf and buf[-1]["datetime"] == candle["datetime"]:
                buf[-1] = candle          # update in-progress candle
            else:
                buf.append(candle)

    def snapshot(self, symbol: str) -> list[dict]:
        with self._lock:
            return list(self._buffer.get(symbol, []))

    def get_and_reset(self, symbol: str) -> list[dict]:
        with self._lock:
            return self._buffer.pop(symbol, [])


# Module-level singleton — imported by main.py WebSocket handler
live_store = LiveCandleStore()


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_collection(tf: str, symbol: str | None = None):
    """
    Return the per-symbol collection  candles_{tf}_{symbol_safe}
    or fall back to flat candles_{tf} for legacy compatibility.
    NOTE: main.py patches load_history() at startup — this default is the fallback.
    """
    import re
    client = MongoClient(MONGO_CREDS_FILE)
    db     = client[MONGO_DB_DEFAULT]
    if symbol:
        safe = re.sub(r"[^A-Za-z0-9]", "_", symbol)
        return db[f"candles_{tf}_{safe}"]
    return db[f"candles_{tf}"]


def load_history(symbol: str, tf: str, n: int = HISTORY_LOOKBACK) -> pd.DataFrame:
    """
    Pull the last `n` candles from MongoDB (fallback implementation).
    main.py replaces this at startup with _load_history_ist which uses
    the correct per-symbol collection and IST-naive datetime handling.
    """
    col  = _get_collection(tf, symbol)
    docs = list(
        col.find(
            {},
            {"_id": 0, "symbol": 0},
            sort=[("datetime", -1)],
        ).limit(n)
    )
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs[::-1])
    df["datetime"] = (
        pd.to_datetime(df["datetime"], utc=True)
        .dt.tz_convert(IST)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Merge historical + live candles
# ─────────────────────────────────────────────────────────────────────────────

def _live_candles_to_df(live: list[dict]) -> pd.DataFrame:
    if not live:
        return pd.DataFrame()
    df = pd.DataFrame(live)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(IST)
    return df


def build_merged_df(symbol: str, tf: str, live_candles: list[dict]) -> pd.DataFrame | None:
    """
    Merge last HISTORY_LOOKBACK historical candles with live WebSocket candles.
    Recalculate EMA indicators on the full merged series.
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

    merged = (
        merged
        .drop_duplicates(subset=["datetime"])
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
    """Min of open and close — the candle body's lowest price."""
    return min(row["open"], row["close"])


# ─────────────────────────────────────────────────────────────────────────────
# Core: identify waves in chronological order (Wave 1 = oldest, Wave N = newest)
# ─────────────────────────────────────────────────────────────────────────────

def _identify_waves(
    df:          pd.DataFrame,
    target_date: date,
    max_waves:   int,
) -> list[Wave]:
    """
    Scan forward through all candles up to target_date and collect up to
    `max_waves` normal waves plus their intervening counter waves.

    Returns Wave objects in CHRONOLOGICAL order (oldest Wave 1 first):
        Wave(normal,1), Wave(counter,1), Wave(normal,2), Wave(counter,2), …

    Wave-low cluster rule:
        Run of consecutive candles whose body-low (min of open, close)
        is BELOW ema9_low forms one pivot cluster.
        Wave-low = candle with the LOWEST body-low in that cluster.

    Wave-high rule:
        Highest wick-high across all candles strictly between the end of
        pivot cluster[i] and the start of pivot cluster[i+1].

    Counter wave:
        From normal_wave[N].high  →  normal_wave[N+1].low
        size = abs(high − low)
    """

    # ── 1. Candles up to and including target_date ────────────────────────────
    df_scan = df[df["datetime"].dt.date <= target_date].reset_index(drop=True)
    if df_scan.empty:
        return []

    rows = df_scan.to_dict("records")
    n    = len(rows)

    # ── 2. Mark each bar: is body-low below ema9_low? ─────────────────────────
    below: list[bool] = []
    for row in rows:
        ema_l = row.get("ema9_low")
        if ema_l is None or (isinstance(ema_l, float) and ema_l != ema_l):
            below.append(False)
        else:
            below.append(_body_low(row) < ema_l)

    # ── 3. Group runs of below-EMA bars into pivot clusters ───────────────────
    clusters: list[tuple[int, int]] = []   # (start_idx, end_idx) inclusive
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
        return []   # need ≥ 2 pivots to form even one wave

    # ── 4. Per cluster: pick candle with LOWEST body-low ─────────────────────
    pivots: list[dict] = []   # {cluster_start, cluster_end, low, low_dt}
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

    # ── 5. Build normal waves from consecutive pivot pairs ────────────────────
    #       Wave-high = highest wick-high in the inter-cluster gap
    normal_raws: list[dict] = []   # {low, low_dt, high, high_dt, size}

    for p_idx in range(len(pivots) - 1):
        p_curr = pivots[p_idx]
        p_next = pivots[p_idx + 1]

        # Candles strictly BETWEEN the two pivot clusters
        seg_start = p_curr["cluster_end"] + 1
        seg_end   = p_next["cluster_start"] - 1

        if seg_start > seg_end:
            # Clusters are adjacent — no gap candles; skip this pair
            continue

        # Highest wick-high in gap
        wave_high    = rows[seg_start]["high"]
        wave_high_dt = _dt_str(rows[seg_start]["datetime"])
        for k in range(seg_start + 1, seg_end + 1):
            if rows[k]["high"] > wave_high:
                wave_high    = rows[k]["high"]
                wave_high_dt = _dt_str(rows[k]["datetime"])

        normal_raws.append({
            "low":     p_curr["low"],
            "low_dt":  p_curr["low_dt"],
            "high":    wave_high,
            "high_dt": wave_high_dt,
            "size":    abs(wave_high - p_curr["low"]),
        })

        if len(normal_raws) >= max_waves:
            break

    if not normal_raws:
        return []

    # ── 6. Interleave normal + counter waves; assign chronological numbers ────
    #
    #   Layout:
    #     Wave(normal, 1)        ← oldest normal wave
    #     Wave(counter, 1)       ← pullback after Wave 1
    #     Wave(normal, 2)
    #     Wave(counter, 2)
    #     ...
    #     Wave(normal, N)        ← newest normal wave (no trailing counter)
    #
    all_waves: list[Wave] = []

    for idx, nw in enumerate(normal_raws):
        wave_num = idx + 1   # 1-based, chronological

        all_waves.append(Wave(
            wave_type = WAVE_NORMAL,
            wave_num  = wave_num,
            low       = nw["low"],
            low_dt    = nw["low_dt"],
            high      = nw["high"],
            high_dt   = nw["high_dt"],
            size      = nw["size"],
        ))

        # Counter wave between wave[idx] and wave[idx+1]
        if idx + 1 < len(normal_raws):
            nw_next = normal_raws[idx + 1]

            # Counter wave: from this wave's high down to next wave's low
            # high of counter = whichever is higher (nw.high vs nw_next.low)
            # low  of counter = whichever is lower
            cw_lo_val = min(nw["high"], nw_next["low"])
            cw_hi_val = max(nw["high"], nw_next["low"])
            cw_lo_dt  = nw["high_dt"]    if nw["high"]     <= nw_next["low"] else nw_next["low_dt"]
            cw_hi_dt  = nw_next["low_dt"] if nw["high"]    <= nw_next["low"] else nw["high_dt"]

            all_waves.append(Wave(
                wave_type = WAVE_COUNTER,
                wave_num  = wave_num,   # counterWave 1 follows Wave 1, etc.
                low       = cw_lo_val,
                low_dt    = cw_lo_dt,
                high      = cw_hi_val,
                high_dt   = cw_hi_dt,
                size      = abs(cw_hi_val - cw_lo_val),
            ))

    # Chronological order is already guaranteed by the forward scan
    return all_waves


# ─────────────────────────────────────────────────────────────────────────────
# Mother Wave selection
# ─────────────────────────────────────────────────────────────────────────────

def _select_mother_waves(
    waves: list[Wave],
) -> tuple[Optional[Wave], Optional[Wave], Optional[Wave]]:
    """
    Returns (motherwave, second_wave, third_wave).

    Motherwave   = largest size (oldest of the three by design)
    2nd-largest  = second largest size (any wave type)
    3x-smaller   = among all same-type waves (excl. motherwave & 2nd-largest)
                   whose size ≤ motherwave.size / 3, pick the one whose size
                   is CLOSEST TO motherwave/3 (i.e. the largest qualifying one).
                   Break ties by recency (closest to run-day).
    """
    if not waves:
        return None, None, None

    by_size = sorted(waves, key=lambda w: w.size, reverse=True)

    motherwave  = by_size[0] if len(by_size) >= 1 else None
    second_wave = by_size[1] if len(by_size) >= 2 else None
    third_wave  = None

    if motherwave:
        threshold = motherwave.size / 3.0

        # Collect all qualifying candidates:
        #   - not already selected as motherwave or 2nd-largest
        #   - same wave_type as motherwave
        #   - size <= motherwave.size / 3
        candidates = [
            w for w in waves
            if w is not motherwave
            and w is not second_wave
            and w.wave_type == motherwave.wave_type
            and w.size <= threshold
        ]

        if candidates:
            # Pick the candidate with size CLOSEST to threshold (largest among qualifiers).
            # Among candidates with equal size, pick the most recent (last in chrono order).
            max_qualifying_size = max(c.size for c in candidates)
            # Iterate newest→oldest; first hit with max_qualifying_size is most recent
            for w in reversed(waves):
                if w in candidates and w.size == max_qualifying_size:
                    third_wave = w
                    break

    return motherwave, second_wave, third_wave


# ─────────────────────────────────────────────────────────────────────────────
# Output formatter  — exact layout per user specification
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_selected_wave(display_label: str, w: Wave) -> str:
    """
    Motherwave:
      Wave No. : 3
      Size: 342.34   Higher High → (2026-04-10 10:15) (3450.75)   Lower Low → (2026-04-08 09:15) (3108.41)
    """
    return (
        f"{display_label}:\n"
        f"  Wave No. : {w.wave_num}\n"
        f"  Size: {w.size:,.2f}"
        f"   Higher High → ({w.high_dt}) ({w.high:.2f})"
        f"   Lower Low   → ({w.low_dt}) ({w.low:.2f})"
    )


def _fmt_wave_row(w: Wave) -> str:
    """
    Wave 2:
      Size: 57.34   Higher High 2 → (dateTime) (Value)   Lower Low 2 → (dateTime) (Value)
    """
    if w.wave_type == WAVE_NORMAL:
        row_label = f"Wave {w.wave_num}"
    else:
        row_label = f"counterWave {w.wave_num}"

    return (
        f"{row_label}:\n"
        f"  Size: {w.size:,.2f}"
        f"   Higher High {w.wave_num} → ({w.high_dt}) ({w.high:.2f})"
        f"   Lower Low {w.wave_num}   → ({w.low_dt}) ({w.low:.2f})"
    )


def format_signal(sig: dict) -> str:
    """
    Render the complete output block for one symbol.

    Stock Name: <SYMBOL>
    Motherwave:
      Wave No. : x
      Size: ...   Higher High → ...   Lower Low → ...
    2ndlargestwave:
      Wave No. : x
      Size: ...
    3xsmallerwave:
      Wave No. : x
      Size: ...
    Wave 1:
      Size: ...   Higher High 1 → ...   Lower Low 1 → ...
    counterWave 1:
      Size: ...   Higher High 1 → ...   Lower Low 1 → ...
    Wave 2:
      ...
    """
    lines: list[str] = []

    # Header
    lines.append(f"Stock Name: {sig['symbol']}")

    # ── The three selected waves ──────────────────────────────────────────────
    selected = [
        ("_wave_motherwave",  "Motherwave"),
        ("_wave_second_wave", "2ndlargestwave"),
        ("_wave_third_wave",  "3xsmallerwave"),
    ]
    for key, display_label in selected:
        w = sig.get(key)
        if w is None:
            lines.append(f"{display_label}:\n  (not identified)")
        else:
            lines.append(_fmt_selected_wave(display_label, w))

    # ── Full chronological wave list ─────────────────────────────────────────
    for w in sig.get("_all_waves", []):
        lines.append(_fmt_wave_row(w))

    return "\n".join(lines)


def print_signal(sig: dict) -> None:
    """Print one signal block to stdout."""
    print(format_signal(sig))
    print()   # blank separator between symbols


# ─────────────────────────────────────────────────────────────────────────────
# Public API — scan_symbol  (called by main.py)
# ─────────────────────────────────────────────────────────────────────────────

def scan_symbol(
    symbol:      str,
    df:          pd.DataFrame,
    target_date: date,
) -> list[dict]:
    """
    Run the Mother Wave identification engine on `df` relative to `target_date`.

    Steps
    ─────
    1. Identify up to MOTHERWAVE_LOOKBACK normal waves (+ counter waves)
       by scanning forward through candles up to target_date.
    2. Select motherwave (largest), 2nd-largest, 3x-smaller (closest to run-day,
       same type as motherwave, size ≤ motherwave/3, but LARGEST among qualifiers).
    3. Return a list with one result dict if a motherwave was found, else [].

    Result dict — serialisable keys (for JSON API & CSV):
      symbol, scan_date, total_waves_found
      motherwave_type,  motherwave_wave_num,  motherwave_size,
      motherwave_low,   motherwave_low_dt,    motherwave_high,  motherwave_high_dt
      second_wave_type, second_wave_wave_num, second_wave_size, …
      third_wave_type,  third_wave_wave_num,  third_wave_size,  …

    Private keys (used only by format_signal / print_signal, stripped before JSON):
      _wave_motherwave   → Wave object
      _wave_second_wave  → Wave object
      _wave_third_wave   → Wave object
      _all_waves         → list[Wave] in chronological order
    """
    waves = _identify_waves(df, target_date, max_waves=MOTHERWAVE_LOOKBACK)
    if not waves:
        return []

    motherwave, second_wave, third_wave = _select_mother_waves(waves)
    if not motherwave:
        return []

    def _flat(prefix: str, w: Optional[Wave]) -> dict:
        if w is None:
            return {
                f"{prefix}_type":     None,
                f"{prefix}_wave_num": None,
                f"{prefix}_size":     None,
                f"{prefix}_low":      None,
                f"{prefix}_low_dt":   None,
                f"{prefix}_high":     None,
                f"{prefix}_high_dt":  None,
            }
        return {
            f"{prefix}_type":     w.wave_type,
            f"{prefix}_wave_num": w.wave_num,
            f"{prefix}_size":     round(w.size,  4),
            f"{prefix}_low":      round(w.low,   4),
            f"{prefix}_low_dt":   w.low_dt,
            f"{prefix}_high":     round(w.high,  4),
            f"{prefix}_high_dt":  w.high_dt,
        }

    result = {
        "symbol":            symbol,
        "scan_date":         str(target_date),
        "total_waves_found": len(waves),

        # Flat serialisable fields (JSON API + CSV)
        **_flat("motherwave",  motherwave),
        **_flat("second_wave", second_wave),
        **_flat("third_wave",  third_wave),

        # Private objects for the terminal formatter (not sent over JSON)
        "_wave_motherwave":  motherwave,
        "_wave_second_wave": second_wave,
        "_wave_third_wave":  third_wave,
        "_all_waves":        waves,
    }
    return [result]


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: scan a symbol live  (historical MongoDB + WebSocket buffer)
# ─────────────────────────────────────────────────────────────────────────────

def scan_symbol_live(
    symbol:      str,
    tf:          str,
    target_date: date,
) -> list[dict]:
    """
    High-level call used by the live scanner loop:
      1. Snapshot live candles from LiveCandleStore (non-destructive)
      2. Merge with historical warm-up candles from MongoDB
      3. Run scan_symbol (mother wave ID) on the merged DataFrame
    """
    live_candles = live_store.snapshot(symbol)
    df = build_merged_df(symbol, tf, live_candles)
    if df is None:
        return []
    return scan_symbol(symbol, df, target_date)