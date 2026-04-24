# ================= MAIN — EMA 9 WAVE SCANNER =================
#
# FastAPI server exposing:
#   GET  /                → health check
#   GET  /signals         → latest scan results (JSON)
#   WS   /ws              → real-time signal stream (JSON push on each new signal)
#
# Internal scanner loop (same architecture as original main.py):
#   fetch → build_df → calculate_indicators → scan_symbol
#
# Modes (set in config.py):
#   LIVE   — real-time, polls every candle close
#   SINGLE — backtest a specific date
#   RANGE  — backtest a date range
#
# Run:
#   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# ==============================================================

from __future__ import annotations

import asyncio
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, date as _date
from typing import Any

import pandas as pd
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import (
    IST, TIMEFRAME, FETCH_DAYS, EMA_PERIOD,
    SAVE_SIGNALS_TO_CSV, CSV_OUTPUT_PATH,
    SEND_TELEGRAM, OVERRIDE_TRADING_DAY, OVERRIDE_DATE_RANGE,
    ASYNC_MAX_CONCURRENT, API_HOST, API_PORT,
)
from fyers_client import get_fyers, check_token_mid_run
from symbol_loader import load_symbols
from time_utils import get_last_trading_day, get_last_closed_candle_time, is_trading_day
from indicators import calculate_indicators
from strategy import scan_symbol
from signal_formatter import print_signal, print_scan_summary
from telegram_utils import send_signal, send_startup_message

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="EMA 9 Wave Scanner", version="1.0.0")

# ── Global state (shared between scanner loop and API) ───────────────────────
_latest_signals: list[dict] = []
_scan_running:   bool       = False
_state_lock      = threading.Lock()
_ws_clients:     list[WebSocket] = []
_ws_lock         = asyncio.Lock()

# ── Mode detection ────────────────────────────────────────────────────────────
if OVERRIDE_DATE_RANGE:
    _range_start = _date.fromisoformat(OVERRIDE_DATE_RANGE[0])
    _range_end   = _date.fromisoformat(OVERRIDE_DATE_RANGE[1])
    RUN_MODE     = "RANGE"
elif OVERRIDE_TRADING_DAY:
    RUN_MODE = "SINGLE"
else:
    RUN_MODE = "LIVE"

print(f"\n  EMA 9 WAVE SCANNER — Mode: {RUN_MODE}\n")

fyers   = get_fyers()
symbols = load_symbols()

if SEND_TELEGRAM:
    send_startup_message()

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _eod_dt(d: _date) -> datetime:
    return IST.localize(
        datetime.combine(d, datetime.min.time())
        .replace(hour=15, minute=30, second=0, microsecond=0)
    )


def backtest_dates() -> list[_date]:
    if RUN_MODE == "SINGLE":
        return [_date.fromisoformat(OVERRIDE_TRADING_DAY)]
    dates, cur = [], _range_start
    while cur <= _range_end:
        if is_trading_day(cur):
            dates.append(cur)
        cur += timedelta(days=1)
    return dates


def build_df(response: dict, last_closed: datetime) -> pd.DataFrame | None:
    candles = response.get("candles", [])
    if not candles:
        return None
    df = pd.DataFrame(candles, columns=["ts", "open", "high", "low", "close", "vol"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
    df = df.drop(columns="ts").sort_values("datetime").reset_index(drop=True)
    df = df[df["datetime"] <= last_closed]
    if len(df) < EMA_PERIOD:
        return None
    return calculate_indicators(df)


# ─────────────────────────────────────────────────────────────────────────────
# Worker: fetch + detect
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_and_process(
    symbol:       str,
    last_closed:  datetime,
    target_date:  _date,
    results:      list,
    results_lock: threading.Lock,
) -> str:
    """
    Runs in a thread-pool worker.
    Returns "ok" | "expired" | "429"
    """
    try:
        response = fyers.history({
            "symbol":     symbol,
            "resolution": TIMEFRAME,
            "date_format": "1",
            "range_from": (last_closed - timedelta(days=FETCH_DAYS)).strftime("%Y-%m-%d"),
            "range_to":   last_closed.strftime("%Y-%m-%d"),
            "cont_flag":  "1",
        })
    except Exception as e:
        print(f"\n  [{symbol}] Request exception: {e}")
        return "ok"

    if response.get("s") != "ok":
        if check_token_mid_run(response):
            return "expired"
        code = str(response.get("code", ""))
        if code in ("-300", "300"):
            return "ok"
        if code == "429":
            return "429"
        return "ok"

    df = build_df(response, last_closed)
    if df is None:
        return "ok"

    signals = scan_symbol(symbol, df, target_date)
    if signals:
        with results_lock:
            results.extend(signals)

    return "ok"


# ─────────────────────────────────────────────────────────────────────────────
# Async orchestrator (mirrors original run_concurrent)
# ─────────────────────────────────────────────────────────────────────────────

async def _run_concurrent(
    to_fetch:     list[str],
    last_closed:  datetime,
    target_date:  _date,
    results:      list,
    results_lock: threading.Lock,
    is_range:     bool,
) -> bool:
    loop          = asyncio.get_running_loop()
    executor      = ThreadPoolExecutor(max_workers=ASYNC_MAX_CONCURRENT)
    token_expired = [False]
    rl_q:         list[str] = []
    total         = len(to_fetch)
    done_ctr      = [0]
    done_lock     = asyncio.Lock()
    rl_lock       = asyncio.Lock()
    sem           = asyncio.Semaphore(ASYNC_MAX_CONCURRENT)

    async def _one(symbol: str):
        async with sem:
            result = await loop.run_in_executor(
                executor, _fetch_and_process,
                symbol, last_closed, target_date, results, results_lock,
            )
        if result == "expired":
            token_expired[0] = True
        elif result == "429":
            async with rl_lock:
                rl_q.append(symbol)
        if not is_range:
            async with done_lock:
                done_ctr[0] += 1
                pct = done_ctr[0] * 100 // total
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                sys.stdout.write(f"\r  [{bar}] {done_ctr[0]}/{total} ")
                sys.stdout.flush()

    await asyncio.gather(*[_one(s) for s in to_fetch])

    # Retry rate-limited symbols
    _RETRY_WAITS = [2.0, 4.0, 6.0, 8.0]
    retry_round  = 0
    while rl_q and not token_expired[0]:
        wait_s = _RETRY_WAITS[min(retry_round, len(_RETRY_WAITS) - 1)]
        await asyncio.sleep(wait_s)
        next_q:  list[str] = []
        sem_r  = asyncio.Semaphore(max(1, ASYNC_MAX_CONCURRENT // 2))
        nq_lock = asyncio.Lock()

        async def _retry(symbol: str, _sem=sem_r):
            async with _sem:
                result = await loop.run_in_executor(
                    executor, _fetch_and_process,
                    symbol, last_closed, target_date, results, results_lock,
                )
            if result == "expired":
                token_expired[0] = True
            elif result == "429":
                async with nq_lock:
                    next_q.append(symbol)

        await asyncio.gather(*[_retry(s) for s in rl_q])
        rl_q = next_q
        retry_round += 1

    if not is_range:
        sys.stdout.write("\r" + " " * 60 + "\r")

    executor.shutdown(wait=False)
    return token_expired[0]


# ─────────────────────────────────────────────────────────────────────────────
# scan_all
# ─────────────────────────────────────────────────────────────────────────────

def scan_all(
    symbol_list:  list[str],
    last_closed:  datetime,
    target_date:  _date,
    is_range:     bool = False,
) -> tuple[list, bool]:
    results      = []
    results_lock = threading.Lock()
    if not is_range:
        print(f"  Fetching {len(symbol_list)} symbol(s) [concurrency={ASYNC_MAX_CONCURRENT}]")
    token_expired = asyncio.run(_run_concurrent(
        symbol_list, last_closed, target_date,
        results, results_lock, is_range,
    ))
    return results, token_expired


# ─────────────────────────────────────────────────────────────────────────────
# Broadcast to WebSocket clients
# ─────────────────────────────────────────────────────────────────────────────

async def _broadcast(data: dict):
    """Push a signal to all connected WebSocket clients."""
    disconnected = []
    for client in list(_ws_clients):
        try:
            await client.send_json(data)
        except Exception:
            disconnected.append(client)
    for c in disconnected:
        _ws_clients.remove(c)


def _broadcast_sync(data: dict):
    """Thread-safe bridge to schedule broadcast from scanner thread."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(_broadcast(data), loop)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# run_one_day
# ─────────────────────────────────────────────────────────────────────────────

def run_one_day(
    target_date: _date,
    last_closed: datetime,
    is_backtest: bool,
    is_range:    bool = False,
) -> list[dict]:
    global _latest_signals

    mode_tag = "[BACKTEST]" if is_backtest else "[LIVE]"

    if not is_range:
        print(f"\n  {'=' * 55}")
        print(f"  Target date : {target_date} {mode_tag}")
        print(f"  Last closed : {last_closed.strftime('%Y-%m-%d %H:%M')}")
        print(f"  Symbols     : {len(symbols)}")
        print(f"  {'=' * 55}")

    t0 = time.time()
    results, token_expired = scan_all(symbols, last_closed, target_date, is_range=is_range)

    if token_expired:
        print("  Token expired — auto-refreshing...")
        global fyers
        fyers = get_fyers()
        print("  Token refreshed — re-running...")
        results, _ = scan_all(symbols, last_closed, target_date, is_range=is_range)

    scan_secs = time.time() - t0
    results.sort(key=lambda x: x["symbol"])

    # Update global latest
    with _state_lock:
        _latest_signals = results

    # Terminal output + Telegram + WebSocket push
    if results:
        print(f"\n  EMA 9 Wave Signals — {target_date}")
        for sig in results:
            print_signal(sig)
            if SEND_TELEGRAM:
                send_signal(sig)
            _broadcast_sync(sig)
    else:
        if not is_range:
            print("\n  (no valid wave setups found this scan)")

    if not is_range:
        print_scan_summary(
            total_symbols=len(symbols),
            signals_found=len(results),
            scan_secs=scan_secs,
            target_date=target_date,
            mode=mode_tag,
        )

    # CSV export
    if SAVE_SIGNALS_TO_CSV and results:
        _save_csv(results)

    return results


def _save_csv(signals: list[dict]) -> None:
    import csv, os
    os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
    write_header = not os.path.exists(CSV_OUTPUT_PATH)
    with open(CSV_OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "symbol",
            "p0_dt", "p0_val", "p1_dt", "p1_val",
            "p2_dt", "p2_val", "p3_dt", "p3_val",
            "p4_dt", "p4_val", "fib_ext_price",
            "entry_candle",
        ])
        if write_header:
            writer.writeheader()
        for sig in signals:
            row = dict(sig)
            ec  = row.get("entry_candle")
            row["entry_candle"] = (
                f"{ec['color']}({ec['candle_no']}) {ec['datetime']}" if ec else ""
            )
            writer.writerow(row)


# ─────────────────────────────────────────────────────────────────────────────
# Scanner background thread
# ─────────────────────────────────────────────────────────────────────────────

def _scanner_loop():
    global _scan_running

    if RUN_MODE in ("SINGLE", "RANGE"):
        dates    = backtest_dates()
        is_range = RUN_MODE == "RANGE"
        print(f"  Backtest dates: {[str(d) for d in dates]}\n")
        for target_date in dates:
            run_one_day(target_date, _eod_dt(target_date), is_backtest=True, is_range=is_range)
        print(f"\n  Backtest complete — {len(dates)} day(s) processed.\n")
        return

    # LIVE mode
    last_processed = None
    while True:
        now          = datetime.now(IST)
        last_closed  = get_last_closed_candle_time(now)
        target_date  = get_last_trading_day(now)
        tf           = int(TIMEFRAME)
        minute_block = (now.minute // tf) * tf
        current_candle = now.replace(minute=minute_block, second=0, microsecond=0)

        if last_processed == current_candle:
            time.sleep(1)
            continue

        last_processed = current_candle
        _scan_running  = True

        run_one_day(target_date, last_closed, is_backtest=False)

        _scan_running = False

        for sec in range(5, 0, -1):
            sys.stdout.write(f"\r  Next scan in {sec}s ")
            sys.stdout.flush()
            time.sleep(1)
        print()


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def health():
    return {"status": "ok", "strategy": "EMA 9 Wave (-0.236)", "mode": RUN_MODE}


@app.get("/signals")
async def get_signals():
    """Return latest scan results as JSON."""
    with _state_lock:
        return JSONResponse(content={
            "count":   len(_latest_signals),
            "signals": _latest_signals,
        })


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint — client connects and receives JSON pushes for each
    new signal as it is detected (real-time).
    """
    await websocket.accept()
    async with _ws_lock:
        _ws_clients.append(websocket)
    try:
        # Keep alive — send ping every 30s
        while True:
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        async with _ws_lock:
            if websocket in _ws_clients:
                _ws_clients.remove(websocket)


# ─────────────────────────────────────────────────────────────────────────────
# Startup: launch scanner in background thread
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    t = threading.Thread(target=_scanner_loop, daemon=True)
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,
    )