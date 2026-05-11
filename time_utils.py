# ================= TIME UTILS — EMA 9 WAVE STRATEGY =================
# Unchanged structure from original — NSE holiday calendar updated.
# =====================================================================

from datetime import datetime, timedelta, date
from config import IST, TIMEFRAME

NSE_HOLIDAYS = {
    # 2025
    date(2025, 1, 26), date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31),
    date(2025, 4, 10), date(2025, 4, 14), date(2025, 4, 18), date(2025, 5,  1),
    date(2025, 8, 15), date(2025, 8, 27), date(2025, 10, 2), date(2025, 10, 20),
    date(2025, 10, 21), date(2025, 11, 5), date(2025, 12, 25),
    # 2026
    date(2026, 1, 26), date(2026, 3,  3), date(2026, 3, 20), date(2026, 3, 26),
    date(2026, 4,  3), date(2026, 4, 14), date(2026, 5,  1), date(2026, 8, 15),
    date(2026, 10, 2), date(2026, 12, 25),
}

NSE_SPECIAL_OPEN = {
    date(2024, 2, 1),
}


def is_trading_day(d: date) -> bool:
    if d in NSE_SPECIAL_OPEN:
        return True
    if d.weekday() >= 5:
        return False
    if d in NSE_HOLIDAYS:
        return False
    return True


def get_last_trading_day(now: datetime) -> date:
    d = now.date()
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def get_nth_prev_trading_day(ref_date: date, n: int) -> date:
    d = ref_date
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if is_trading_day(d):
            count += 1
    return d


def trading_days_ago(n: int, from_date: date | None = None) -> date:
    """
    Return the date that is exactly `n` trading days before `from_date`
    (defaults to today).  The returned date is itself a trading day.

    Example: trading_days_ago(45) gives the window_start for a 45-trading-day
    rolling preload window.
    """
    ref = from_date if from_date is not None else date.today()
    # Snap ref to the nearest trading day on or before itself
    while not is_trading_day(ref):
        ref -= timedelta(days=1)
    return get_nth_prev_trading_day(ref, n)


def get_last_closed_candle_time(now: datetime) -> datetime:
    tf = int(TIMEFRAME)
    last_trading_day = get_last_trading_day(now)

    if now.date() == last_trading_day:
        minutes             = (now.minute // tf) * tf
        current_candle_open = now.replace(minute=minutes, second=0, microsecond=0)
        last                = current_candle_open - timedelta(minutes=tf)
        market_open         = now.replace(hour=9, minute=15, second=0, microsecond=0)

        if last < market_open:
            prev = get_nth_prev_trading_day(last_trading_day, 1)
            last = IST.localize(
                datetime.combine(prev, datetime.min.time())
                .replace(hour=15, minute=30, second=0, microsecond=0)
            )
    else:
        last = IST.localize(
            datetime.combine(last_trading_day, datetime.min.time())
            .replace(hour=15, minute=30, second=0, microsecond=0)
        )

    return last

def get_trading_day_start(n_days: int, from_date: date | None = None) -> datetime:
    """
    Returns the IST-naive datetime of market open (09:15) exactly n_days
    trading days ago, for use as a MongoDB cutoff.

    Returns IST naive (no tzinfo) to match MongoDB storage format.
    """
    ref    = from_date if from_date is not None else date.today()
    cutoff = get_nth_prev_trading_day(ref, n_days)
    return datetime.combine(cutoff, datetime.min.time().replace(hour=9, minute=15))