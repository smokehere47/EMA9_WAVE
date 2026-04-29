# ================= MAIN — EMA 9 WAVE SCANNER =================
#
# Architecture (live mode):
#   • historical_preload.py  → Fyers REST → MongoDB  (run once at startup)
#   • Fyers WebSocket        → live candle ticks → LiveCandleStore (strategy.py)
#   • Scanner loop           → MongoDB (last 1850 candles) + WebSocket buffer
#                              → scan_symbol() → signals
#   • ZERO Fyers REST API calls during scan cycle
#
# Backtest mode: reads purely from MongoDB, no WebSocket needed.
#
# MongoDB layout:
#   DB  : EMA_wave
#   Collections: candle_{tf}  (one per timeframe, all symbols inside)
#   e.g. candle_1, candle_15, candle_60
#   Each document: { symbol, datetime (IST naive), open, high, low, close, volume }
#   Index: unique compound on (symbol, datetime) per collection.
#
# FastAPI routes:
#   GET  /         → health
#   GET  /signals  → latest scan results (JSON)
#   WS   /ws       → real-time signal push per signal
#
# Run:
#   python historical_preload.py   # once before first live run
#   uvicorn main:app --host 0.0.0.0 --port 8000
# ==============================================================

from __future__ import annotations

import asyncio
import os
import re
import signal
import sys
import time
import threading
from datetime import datetime, timedelta, date as _date
from typing import Any

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from config import (
    IST, TIMEFRAME, EMA_PERIOD,
    SAVE_SIGNALS_TO_CSV, CSV_OUTPUT_PATH,
    SEND_TELEGRAM, OVERRIDE_TRADING_DAY, OVERRIDE_DATE_RANGE,
    API_HOST, API_PORT,
    HISTORY_LOOKBACK, HISTORICAL_TIMEFRAMES,
    MONGO_CREDS_FILE, MONGO_DB_DEFAULT, MONGO_COLLECTION_PREFIX,
)
from fyers_client import get_fyers
from symbol_loader import load_symbols
from time_utils import get_last_trading_day, get_last_closed_candle_time, is_trading_day
from strategy import scan_symbol, build_merged_df, live_store
import strategy as _strategy
from signal_formatter import print_scan_summary
from strategy import print_signal   # strategy owns the formatter now

if SEND_TELEGRAM:
    from telegram_utils import send_signal, send_startup_message


# ─────────────────────────────────────────────────────────────────────────────
# Global stop event — set on Ctrl+C / SIGTERM to cleanly stop all threads
# ─────────────────────────────────────────────────────────────────────────────

_stop_event  = threading.Event()
_ws_instance = None   # Fyers WebSocket reference for clean close on shutdown


def _shutdown(sig, frame):
    """Signal handler for SIGINT / SIGTERM — stops all threads cleanly."""
    print("\n\n  Shutting down (Ctrl+C)…")
    _stop_event.set()

    global _ws_instance
    if _ws_instance is not None:
        try:
            _ws_instance.close()
        except Exception:
            pass

    # Hard kill after 3 s if threads are still lingering
    threading.Timer(3.0, lambda: os._exit(0)).start()


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: strip private Wave objects before JSON serialisation
# ─────────────────────────────────────────────────────────────────────────────

def _public(sig: dict) -> dict:
    """Return a copy of the signal dict with all private '_*' keys removed."""
    return {k: v for k, v in sig.items() if not k.startswith("_")}


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB credentials — loaded from plain-text file (no creds in source code)
# ─────────────────────────────────────────────────────────────────────────────

def _load_mongo_creds(creds_file: str) -> tuple[str, str]:
    """
    Read MongoDB URI and database name from a plain-text credentials file.

    Expected file format (one key=value per line, no quotes needed):
        uri=mongodb://username:password@host:27017
        db=EMA_wave

    Returns (uri, db_name).  Raises FileNotFoundError / ValueError on failure.
    """
    creds_path = os.path.abspath(creds_file)
    if not os.path.isfile(creds_path):
        raise FileNotFoundError(
            f"MongoDB credentials file not found: {creds_path}\n"
            f"Create the file with:\n  uri=mongodb://<host>:<port>\n  db=<database_name>"
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
        raise ValueError(
            f"'uri' key missing in credentials file: {creds_path}"
        )

    db_name = parsed.get("db", MONGO_DB_DEFAULT)
    return uri, db_name


# Load at import time so the rest of the module can use MONGO_URI / MONGO_DB
try:
    MONGO_URI, MONGO_DB = _load_mongo_creds(MONGO_CREDS_FILE)
    print(f"  [MongoDB] Credentials loaded from: {MONGO_CREDS_FILE}")
    print(f"  [MongoDB] Database : {MONGO_DB}")
except (FileNotFoundError, ValueError) as _creds_err:
    print(f"\n  ❌ {_creds_err}\n")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Collection naming helper
# ─────────────────────────────────────────────────────────────────────────────

def collection_name(tf: str) -> str:
    """
    Return the MongoDB collection name for a given timeframe.
    Pattern:  candle_{tf}
    Example:  candle_15   (holds ALL symbols for the 15-min TF)
    """
    return f"{MONGO_COLLECTION_PREFIX}_{tf}"


# ─────────────────────────────────────────────────────────────────────────────
# Patch strategy.load_history to use new candle_{tf} collections
# ─────────────────────────────────────────────────────────────────────────────

from pymongo import MongoClient
import pandas as pd


def _load_history_ist(symbol: str, tf: str, n: int = None) -> pd.DataFrame:
    """
    Pull the last `n` candles for (symbol, tf) from the shared candle_{tf} collection.
    Filters by symbol field; datetimes stored as IST-naive, attached with tz_localize.
    """
    from config import HISTORY_LOOKBACK, IST
    if n is None:
        n = HISTORY_LOOKBACK

    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB][collection_name(tf)]

    docs = list(
        col.find(
            {"symbol": symbol},          # filter by symbol field
            {"_id": 0, "symbol": 0},     # exclude _id and symbol from returned docs
            sort=[("datetime", -1)],
        ).limit(n)
    )
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs[::-1])
    # Stored as IST-naive — attach IST timezone directly, no conversion needed
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST)
    return df


_strategy.load_history = _load_history_ist


# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="EMA 9 Wave Scanner", version="2.0.0")

# ── Shared state ──────────────────────────────────────────────────────────────
_latest_signals: list[dict] = []
_state_lock   = threading.Lock()
_ws_clients:  list[WebSocket] = []
_ws_lock      = asyncio.Lock()

# ── Mode ──────────────────────────────────────────────────────────────────────
if OVERRIDE_DATE_RANGE:
    _range_start = _date.fromisoformat(OVERRIDE_DATE_RANGE[0])
    _range_end   = _date.fromisoformat(OVERRIDE_DATE_RANGE[1])
    RUN_MODE = "RANGE"
elif OVERRIDE_TRADING_DAY:
    RUN_MODE = "SINGLE"
else:
    RUN_MODE = "LIVE"

print(f"\n  EMA 9 WAVE SCANNER — Mode: {RUN_MODE}\n")

symbols = load_symbols()

if SEND_TELEGRAM:
    send_startup_message()


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket feed — started only in LIVE mode
# ─────────────────────────────────────────────────────────────────────────────

def _start_websocket_feed():
    """
    Connect Fyers WebSocket and pipe ticks into live_store.
    Runs in its own daemon thread.
    Stops cleanly when _stop_event is set.
    """
    global _ws_instance
    try:
        from fyers_apiv3.FyersWebsocket import data_ws

        fyers  = get_fyers()
        tf_int = int(TIMEFRAME)

        def _on_message(msg):
            if _stop_event.is_set():
                return
            try:
                sym  = msg.get("symbol", "")
                ltp  = msg.get("ltp", 0.0)
                ts   = msg.get("timestamp", 0)
                candle_open_ts = (ts // (tf_int * 60)) * (tf_int * 60)
                dt_ist = datetime.fromtimestamp(candle_open_ts, tz=IST)
                candle = {
                    "datetime": dt_ist.isoformat(),
                    "open":     msg.get("open_price", ltp),
                    "high":     msg.get("high_price", ltp),
                    "low":      msg.get("low_price",  ltp),
                    "close":    ltp,
                    "volume":   msg.get("vol_traded_today", 0),
                }
                live_store.push(sym, candle)
            except Exception:
                pass

        def _on_error(msg):
            if not _stop_event.is_set():
                print(f"  [WS] Error: {msg}")

        def _on_close(msg):
            if not _stop_event.is_set():
                print(f"  [WS] Closed: {msg}")

        def _on_open():
            if _stop_event.is_set():
                return
            print("  [WS] Connected — subscribing to symbols…")
            sym_tokens = [f"NSE:{s.split(':')[-1]}" if ":" not in s else s
                          for s in symbols]
            ws.subscribe(symbols=sym_tokens, data_type="SymbolUpdate")
            ws.keep_running()

        ws = data_ws.FyersDataSocket(
            access_token=fyers.token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=_on_open,
            on_close=_on_close,
            on_error=_on_error,
            on_message=_on_message,
        )

        _ws_instance = ws

        if not _stop_event.is_set():
            ws.connect()

    except Exception as exc:
        if not _stop_event.is_set():
            print(f"  [WS] Failed to start WebSocket: {exc}")
            print("  [WS] Live candle data unavailable — signals from MongoDB only.")


# ─────────────────────────────────────────────────────────────────────────────
# Scan one symbol (MongoDB + live buffer)
# ─────────────────────────────────────────────────────────────────────────────

def _scan_symbol(symbol: str, target_date: _date, is_live: bool = False) -> list[dict]:
    live_candles = live_store.snapshot(symbol) if is_live else []
    df = build_merged_df(symbol, TIMEFRAME, live_candles)
    if df is None:
        return []
    return scan_symbol(symbol, df, target_date)


# ─────────────────────────────────────────────────────────────────────────────
# Broadcast helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _broadcast(data: dict):
    dead = []
    for client in list(_ws_clients):
        try:
            await client.send_json(data)
        except Exception:
            dead.append(client)
    for c in dead:
        _ws_clients.remove(c)


def _broadcast_sync(data: dict):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(_broadcast(data), loop)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# run_one_day — no Fyers REST calls
# ─────────────────────────────────────────────────────────────────────────────

def run_one_day(
    target_date: _date,
    is_backtest: bool,
    is_range:    bool = False,
) -> list[dict]:
    global _latest_signals

    mode_tag = "[BACKTEST]" if is_backtest else "[LIVE]"
    is_live  = not is_backtest

    if not is_range:
        print(f"\n  {'=' * 55}")
        print(f"  Target date : {target_date}  {mode_tag}")
        print(f"  Symbols     : {len(symbols)}")
        print(f"  Data source : MongoDB ({collection_name(TIMEFRAME)}) last {HISTORY_LOOKBACK} candles" +
              (" + WebSocket" if is_live else ""))
        print(f"  {'=' * 55}")

    t0      = time.time()
    results = []
    total   = len(symbols)

    for idx, symbol in enumerate(symbols, 1):
        if _stop_event.is_set():
            break
        sigs = _scan_symbol(symbol, target_date, is_live=is_live)
        if sigs:
            results.extend(sigs)

        if not is_range:
            pct = idx * 100 // total
            bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
            sys.stdout.write(f"\r  [{bar}] {idx}/{total} ")
            sys.stdout.flush()

    if not is_range:
        sys.stdout.write("\r" + " " * 60 + "\r")

    scan_secs = time.time() - t0
    results.sort(key=lambda x: x["symbol"])

    with _state_lock:
        _latest_signals = [_public(s) for s in results]

    if results:
        print(f"\n  EMA 9 Wave Signals — {target_date}")
        for sig in results:
            print_signal(sig)
            if SEND_TELEGRAM:
                send_signal(_public(sig))
            _broadcast_sync(_public(sig))
    elif not is_range:
        print("\n  (no valid mother wave setups found this scan)")

    if not is_range:
        print_scan_summary(
            total_symbols=len(symbols),
            signals_found=len(results),
            scan_secs=scan_secs,
            target_date=target_date,
            mode=mode_tag,
        )

    if SAVE_SIGNALS_TO_CSV and results:
        _save_csv(results)

    return results


def _save_csv(signals: list[dict]) -> None:
    import csv
    os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
    write_header = not os.path.exists(CSV_OUTPUT_PATH)
    with open(CSV_OUTPUT_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "symbol",
            "motherwave_type", "motherwave_size",
            "motherwave_low_dt", "motherwave_low",
            "motherwave_high_dt", "motherwave_high",
            "second_wave_type", "second_wave_size",
            "third_wave_type", "third_wave_size",
            "scan_date",
        ])
        if write_header:
            writer.writeheader()
        writer.writerows(signals)


# ─────────────────────────────────────────────────────────────────────────────
# Backtest helpers
# ─────────────────────────────────────────────────────────────────────────────

def _eod_dt(d: _date) -> datetime:
    return IST.localize(
        datetime.combine(d, datetime.min.time())
        .replace(hour=15, minute=30)
    )


def _backtest_dates() -> list[_date]:
    if RUN_MODE == "SINGLE":
        return [_date.fromisoformat(OVERRIDE_TRADING_DAY)]
    dates, cur = [], _range_start
    while cur <= _range_end:
        if is_trading_day(cur):
            dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ─────────────────────────────────────────────────────────────────────────────
# Scanner loop
# ─────────────────────────────────────────────────────────────────────────────

def _scanner_loop():
    if RUN_MODE in ("SINGLE", "RANGE"):
        dates    = _backtest_dates()
        is_range = RUN_MODE == "RANGE"
        print(f"  Backtest dates: {[str(d) for d in dates]}\n")
        for target_date in dates:
            if _stop_event.is_set():
                break
            run_one_day(target_date, is_backtest=True, is_range=is_range)
        print(f"\n  Backtest complete — {len(dates)} day(s) processed.\n")
        return

    # LIVE mode — wait for each candle close then scan
    last_processed = None
    tf_int = int(TIMEFRAME)

    while not _stop_event.is_set():
        now          = datetime.now(IST)
        target_date  = get_last_trading_day(now)
        minute_block = (now.minute // tf_int) * tf_int
        current_candle = now.replace(minute=minute_block, second=0, microsecond=0)

        if last_processed == current_candle:
            _stop_event.wait(timeout=1)
            continue

        last_processed = current_candle
        run_one_day(target_date, is_backtest=False)

        if _stop_event.is_set():
            break

        for sec in range(5, 0, -1):
            if _stop_event.is_set():
                break
            sys.stdout.write(f"\r  Next scan in {sec}s ")
            sys.stdout.flush()
            _stop_event.wait(timeout=1)
        if not _stop_event.is_set():
            print()


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def health():
    return {"status": "ok", "strategy": "EMA 9 Wave — Mother Wave ID", "mode": RUN_MODE}


@app.get("/signals")
async def get_signals():
    with _state_lock:
        return JSONResponse(content={
            "count":   len(_latest_signals),
            "signals": _latest_signals,
        })


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with _ws_lock:
        _ws_clients.append(websocket)
    try:
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
# Startup
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    if RUN_MODE == "LIVE":
        ws_thread = threading.Thread(target=_start_websocket_feed, daemon=True)
        ws_thread.start()
        time.sleep(2)   # give WS a moment to connect before first scan

    scanner_thread = threading.Thread(target=_scanner_loop, daemon=True)
    scanner_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host=API_HOST, port=API_PORT, reload=False)