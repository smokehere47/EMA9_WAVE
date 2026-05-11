# ================= CONFIG — EMA 9 WAVE STRATEGY (−0.236 Level) =================
#
# This is the CORE config file shared across all future strategies.
# All credentials, paths, flags, and tuning parameters live here.
# No other file should hard-code credentials or paths.
# ================================================================================

import pytz

# ── Fyers token files ──────────────────────────────────────────────────────────
CLIENT_ID_FILE      = r"C:/Users/www.abcom.in/9 EMA Low Signal Hourly TF/fyers_client_id.txt"
ACCESS_TOKEN_FILE   = r"C:/Users/www.abcom.in/9 EMA Low Signal Hourly TF/fyers_access_token.txt"
REFRESH_TOKEN_FILE  = r"C:/Users/www.abcom.in/9 EMA Low Signal Hourly TF/fyers_refresh_token.txt"

# SHA-256 of app_id:app_secret
FYERS_APP_ID_HASH   = "XXXXXXXXXXXXXXXXX"   # ← replace with your actual hash
FYERS_PIN           = "XXXXXXXXX"           # ← replace with your Fyers PIN

# ── Symbol source ──────────────────────────────────────────────────────────────
INPUT_EXCEL    = r"C:\Users\www.abcom.in\9 EMA Low Signal Hourly TF\NIFTY.xlsx"
SYMBOL_COLUMN  = "symbol"

# ── Historical preload batch tuning ───────────────────────────────────────────
PRELOAD_BATCH_SIZE  = 3    # symbols fetched simultaneously per batch
PRELOAD_BATCH_PAUSE = 1    # seconds to pause between batches (prevents 429)

# ── Development mode ──────────────────────────────────────────────────────────
# DEV_MODE = True  → scan only DEV_SYMBOLS (fast testing, ~5-6 stocks)
# DEV_MODE = False → scan all stocks from INPUT_EXCEL (production)
DEV_MODE = False
DEV_SYMBOLS = [
    # "NSE:ZYDUSLIFE-EQ",
    # "NSE:YESBANK-EQ",
    # "NSE:RELIANCE-EQ",
    # "NSE:INFY-EQ",
    # "NSE:HDFCBANK-EQ",
    # "NSE:TCS-EQ",
    "NSE:NIFTY50-INDEX",
]

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = "your_bot_token_here"
TELEGRAM_CHAT_ID   = "your_chat_id_here"

# ── Timezone ───────────────────────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ── Candle timeframe ──────────────────────────────────────────────────────────
# Controls ALL data fetching, EMA calculation, and strategy logic.
# Valid values: "1", "3", "5", "15", "30", "60" etc.
TIMEFRAME = "3"   # ← Change to "1" or "5" as needed

# ── How many calendar days of OHLC history to fetch per symbol ────────────────
FETCH_DAYS = 10

# ── EMA period ────────────────────────────────────────────────────────────────
EMA_PERIOD = 9

# ── Fibonacci level ───────────────────────────────────────────────────────────
FIB_EXTENSION = -0.236

# ── Entry logic flag ─────────────────────────────────────────────────────────
ENABLE_ENTRY = False
ENTRY_MAX_CANDLES = 3

# ── Run-day targeting ─────────────────────────────────────────────────────────
# Three modes — set only ONE, leave others as None.
#
# Live mode (default):
#   OVERRIDE_TRADING_DAY = None
#   OVERRIDE_DATE_RANGE  = None
#
# Single date backtest:
#   OVERRIDE_TRADING_DAY = "2026-04-15"
#   OVERRIDE_DATE_RANGE  = None
#
# Date range backtest:
#   OVERRIDE_TRADING_DAY = None
#   OVERRIDE_DATE_RANGE  = ("2026-03-20", "2026-03-25")
OVERRIDE_TRADING_DAY =  None #"2026-04-06"
OVERRIDE_DATE_RANGE  = None #("2026-03-26", "2026-04-06")

# ── Output flags ──────────────────────────────────────────────────────────────
SEND_TELEGRAM        = False
SAVE_SIGNALS_TO_CSV  = False
CSV_OUTPUT_PATH      = r"C:\Users\www.abcom.in\EMA9_Wave\signals.csv"

# ── FastAPI server ────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000

# ── MongoDB ────────────────────────────────────────────────────────────────────
# Credentials are NOT stored here — read at runtime from a plain-text file.
#
# File format (one key=value per line, no quotes):
#   uri=mongodb://username:password@host:27017
#   db=EMA9_WAVE
#
MONGO_CREDS_FILE        = r"C:\Users\www.abcom.in\Ema9_Wave\mongodblocalhost.txt"
MONGO_DB_DEFAULT        = "EMA9_WAVE"
MONGO_COLLECTION_PREFIX = "candle"   # → candle_1, candle_15, candle_60 …

# ── Historical preload ─────────────────────────────────────────────────────────
#
# HISTORICAL_PRELOAD_MONTHS  (calendar months, not trading days)
# ──────────────────────────────────────────────────────────────
# Controls the initial fetch window for a brand-new / empty database.
#
#   First run (empty DB):
#     window_start = today − HISTORICAL_PRELOAD_MONTHS months  (simple date math)
#     The full range  window_start → today  is fetched from Fyers.
#
#   Every subsequent run (DB already has data):
#     Only the gap between the last stored candle and today is fetched.
#       1 day missing  → 1 day fetched
#       5 days missing → 5 days fetched
#     Data is NEVER deleted; history grows continuously.
#
# Why calendar months instead of trading days?
#   Trading-day counting was needed in the old rolling-window-prune model
#   because the prune cutoff had to be expressed as a trading-day boundary.
#   In accumulation mode:
#     • The initial window is a one-time calendar subtraction — relativedelta
#       handles month arithmetic correctly (e.g. Apr 30 − 2 months = Feb 28).
#     • Incremental updates need no day-counting at all: they simply fetch
#       from (last_stored_date + 1 calendar day) → today.
#   There is no reason to convert months → trading days; doing so only
#   adds complexity and causes the "45-day" confusion you saw.
#
HISTORICAL_PRELOAD_MONTHS = 2   # calendar months for the FIRST-RUN fetch window

# All timeframes to store. Add/remove as needed.
# Valid Fyers resolutions: "1","2","3","5","10","15","20","30","60","120","240"
HISTORICAL_TIMEFRAMES = ["1", "3", "5", "10", "15", "30", "60"]

WAVE_LOOKBACK_DAYS  = 15   # trading days of candle history to fetch
WAVE_OUTPUT_COUNT   = 100   # rolling window — max waves shown in output

# ── Chart engine ───────────────────────────────────────────────────────────────
CHART_HOST = "0.0.0.0"
CHART_PORT = 8001