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
PRELOAD_BATCH_PAUSE = 1   # seconds to pause between batches (prevents 429)

# ── Development mode ──────────────────────────────────────────────────────────
# DEV_MODE = True  → scan only DEV_SYMBOLS (fast testing, ~5-6 stocks)
# DEV_MODE = False → scan all stocks from INPUT_EXCEL (production)
DEV_MODE = True
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
# Valid values: "1", "3", "5"
TIMEFRAME = "15"   # ← Change to "1" or "5" as needed

# ── How many calendar days of OHLC history to fetch per symbol ────────────────
FETCH_DAYS = 10

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
OVERRIDE_TRADING_DAY = None # "2026-04-23"   # e.g. "2026-04-15"
OVERRIDE_DATE_RANGE  = None   # e.g. ("2026-03-20", "2026-03-25")

# ── Output flags ──────────────────────────────────────────────────────────────
SEND_TELEGRAM        = False
SAVE_SIGNALS_TO_CSV  = False
CSV_OUTPUT_PATH      = r"C:\Users\www.abcom.in\EMA9_Wave\signals.csv"


# ── FastAPI server ────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000


# ── MongoDB ────────────────────────────────────────────────────────────────────
# MongoDB credentials are NOT stored here.
# They are read at runtime from a plain-text file whose path is set below.
#
# Format of the credentials file (one key=value per line, no quotes):
#   uri=mongodb://username:password@host:27017
#   db=EMA9_WAVE
#
# If the file contains only a URI line (no db= line), MONGO_DB_DEFAULT is used.
MONGO_CREDS_FILE = r"C:\Users\www.abcom.in\Ema9_Wave\mongodblocalhost.txt"   # change if running remote Mongo
MONGO_DB_DEFAULT  = "EMA9_WAVE"

# ── MongoDB collection naming ──────────────────────────────────────────────────
# Collection name pattern: candle_{tf}
# e.g. candle_1, candle_15, candle_60
# Each collection holds ALL symbols for that timeframe.
# Each document contains: { symbol, datetime, open, high, low, close, volume }
# Index: unique on (symbol, datetime) per collection.
MONGO_COLLECTION_PREFIX = "candle"   # results in candle_1, candle_15, etc.

# ── Historical preload ─────────────────────────────────────────────────────────
#
# HISTORICAL_PRELOAD_DAYS is in TRADING DAYS (not calendar days).
# The preloader converts this to the correct calendar date range by walking
# backwards through the NSE trading calendar.
#
# Rolling window behaviour:
#   • On each run, only MISSING days are fetched (incremental).
#   • Any candles older than HISTORICAL_PRELOAD_DAYS trading days are DELETED
#     so the stored window never grows beyond this limit.
#
HISTORICAL_PRELOAD_DAYS = 45   # trading days to keep in MongoDB
 
# All timeframes to store. Add/remove as needed.
# Valid Fyers resolutions: "1","2","3","5","10","15","20","30","60","120","240"
HISTORICAL_TIMEFRAMES = ["1", "3", "5", "10", "15", "30", "60"]
 
# ── Strategy signal engine ─────────────────────────────────────
# How many historical candles to pull from MongoDB as EMA warm-up
# before appending live candles. 50 is enough for EMA-9 stability.
HISTORY_LOOKBACK = 1850

# ── Mother Wave identification ─────────────────────────────────────────────────
# How many waves (back from run-day) to analyse when identifying the motherwave.
MOTHERWAVE_LOOKBACK = 50

# ── Chart engine ───────────────────────────────────────────────
CHART_HOST = "0.0.0.0"
CHART_PORT = 8001        # separate from main.py's API_PORT (8000)
 