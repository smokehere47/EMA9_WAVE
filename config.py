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

# ── Development mode ──────────────────────────────────────────────────────────
# DEV_MODE = True  → scan only DEV_SYMBOLS (fast testing, ~5-6 stocks)
# DEV_MODE = False → scan all stocks from INPUT_EXCEL (production)
DEV_MODE = True
DEV_SYMBOLS = [
    "NSE:ZYDUSLIFE-EQ",
    "NSE:YESBANK-EQ",
    "NSE:RELIANCE-EQ",
    "NSE:INFY-EQ",
    "NSE:HDFCBANK-EQ",
    "NSE:TCS-EQ",
]

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = "your_bot_token_here"
TELEGRAM_CHAT_ID   = "your_chat_id_here"

# ── Timezone ───────────────────────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ── Candle timeframe ──────────────────────────────────────────────────────────
# Controls ALL data fetching, EMA calculation, and strategy logic.
# Valid values: "1", "3", "5"
TIMEFRAME = "3"   # ← Change to "1" or "5" as needed

# ── How many calendar days of OHLC history to fetch per symbol ────────────────
FETCH_DAYS = 5

# ── EMA period ────────────────────────────────────────────────────────────────
EMA_PERIOD = 9

# ── Fibonacci level ───────────────────────────────────────────────────────────
# Extension level used for Point(3) breakout confirmation.
# -0.236 is the standard Fibonacci extension above Point(1).
FIB_EXTENSION = -0.236

# ── Entry logic flag ─────────────────────────────────────────────────────────
# Set ENABLE_ENTRY = True to activate entry candle detection after Point(4).
# Still under development — set False to signal at Point(4) formation only.
ENABLE_ENTRY = True

# Max candles after Point(4) within which entry candle must appear.
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
OVERRIDE_TRADING_DAY = None   # e.g. "2026-04-15"
OVERRIDE_DATE_RANGE  = None   # e.g. ("2026-03-20", "2026-03-25")

# ── Output flags ──────────────────────────────────────────────────────────────
SEND_TELEGRAM        = False
SAVE_SIGNALS_TO_CSV  = False
CSV_OUTPUT_PATH      = r"C:\Users\www.abcom.in\EMA9_Wave\signals.csv"

# ── Concurrency ───────────────────────────────────────────────────────────────
ASYNC_MAX_CONCURRENT = 10

# ── FastAPI server ────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000