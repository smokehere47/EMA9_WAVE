# ================= INDICATORS — EMA 9 WAVE STRATEGY =================
#
# Calculates all indicators required by the EMA 9 Wave Strategy:
#
#   ema9_low   — EMA(9) of candle LOWS   → lower boundary of EMA zone
#   ema9_high  — EMA(9) of candle HIGHS  → upper boundary of EMA zone
#
# Fibonacci retracement levels are computed on-demand per wave setup
# using the fib_levels() helper (no external pyindicator needed —
# standard Fibonacci math is applied directly).
#
# Fibonacci levels used by the strategy:
#   Level 0     → Point(1) high  (mapped to 0)
#   Level 1     → Point(0) low   (mapped to 1)
#   Level -0.236 → Extension above Point(1) (breakout confirmation for Point(3))
#
# pandas-ta matches the standard EMA formula used by TradingView / Zerodha.
# TIMEFRAME in config.py controls the resolution; indicator logic is TF-agnostic.
# =====================================================================

import pandas as pd
import pandas_ta as ta
from config import EMA_PERIOD, FIB_EXTENSION


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add ema9_low and ema9_high columns to the DataFrame."""
    df = df.copy()
    df["ema9_low"]  = ta.ema(df["low"],  length=EMA_PERIOD)
    df["ema9_high"] = ta.ema(df["high"], length=EMA_PERIOD)
    return df


def fib_levels(point0_low: float, point1_high: float) -> dict:
    """
    Compute Fibonacci retracement and extension levels for a swing.

    Mapping (standard strategy convention):
        Point(0) low  → Fibonacci level 1   (bottom of the swing)
        Point(1) high → Fibonacci level 0   (top of the swing)

    The range is:  swing = point1_high − point0_low

    Levels returned:
        0.000  → point1_high
        0.236  → point1_high − 0.236 * swing
        0.382  → point1_high − 0.382 * swing
        0.500  → point1_high − 0.500 * swing
        0.618  → point1_high − 0.618 * swing
        0.786  → point1_high − 0.786 * swing
        1.000  → point0_low
       -0.236  → point1_high + 0.236 * swing  ← KEY level for Point(3) breakout

    Parameters
    ----------
    point0_low   : float — lowest low of Point(0)
    point1_high  : float — highest high of Point(1)

    Returns
    -------
    dict mapping float level key → price value
    """
    swing = point1_high - point0_low
    if swing <= 0:
        return {}

    levels = {
        0.000:  point1_high,
        0.236:  point1_high - 0.236 * swing,
        0.382:  point1_high - 0.382 * swing,
        0.500:  point1_high - 0.500 * swing,
        0.618:  point1_high - 0.618 * swing,
        0.786:  point1_high - 0.786 * swing,
        1.000:  point0_low,
        -0.236: point1_high + 0.236 * swing,   # Extension (above Point(1))
    }
    return levels


def get_fib_extension_price(point0_low: float, point1_high: float) -> float:
    """
    Convenience wrapper — returns the single −0.236 extension price.
    This is the key level Point(3) must exceed for a valid setup.
    """
    levels = fib_levels(point0_low, point1_high)
    return levels.get(FIB_EXTENSION, point1_high)