# ================= TELEGRAM UTILS — EMA 9 WAVE STRATEGY =================
#
# Sends formatted HTML messages to a Telegram bot.
# Signal messages include:
#   - Symbol with a clickable TradingView link
#   - All 5 wave points with datetime and value
#   - Entry candle details (if ENABLE_ENTRY = True)
# =========================================================================

import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


def tv_link(symbol: str) -> str:
    """
    Builds a TradingView chart URL from a Fyers symbol string.
    e.g.  NSE:PGEL-EQ  →  https://www.tradingview.com/chart/?symbol=NSE:PGEL
    """
    try:
        base      = symbol.split(":")[-1]   # PGEL-EQ
        base      = base.replace("-EQ", "") # PGEL
        tv_symbol = f"NSE:{base}"
    except Exception:
        tv_symbol = symbol
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}"


def send_alert(
    message:   str,
    bot_token: str = TELEGRAM_BOT_TOKEN,
    chat_id:   str = TELEGRAM_CHAT_ID,
) -> bool:
    """
    Sends an HTML-formatted message to Telegram.
    Returns True on success, False on any failure.
    """
    url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id":                  chat_id,
        "text":                     message,
        "parse_mode":               "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code == 200:
            return True
        print(f"❌ Telegram error {r.status_code} → {r.text}")
        return False
    except Exception as e:
        print(f"❌ Telegram exception → {e}")
        return False


def format_signal_message(sig: dict) -> str:
    """
    Formats a wave signal dict into an HTML Telegram message.

    Expected sig fields:
        symbol        : str  — e.g. "NSE:RELIANCE-EQ"
        p0_dt, p0_val : datetime str, float
        p1_dt, p1_val : datetime str, float
        p2_dt, p2_val : datetime str, float
        p3_dt, p3_val : datetime str, float
        p4_dt, p4_val : datetime str, float
        fib_ext_price : float  — the −0.236 level price
        entry_candle  : dict | None  — {candle_no, color, datetime}
    """
    symbol = sig["symbol"]
    link   = tv_link(symbol)

    entry_line = ""
    if sig.get("entry_candle"):
        ec   = sig["entry_candle"]
        col  = ec.get("color", "?")
        cno  = ec.get("candle_no", "?")
        cdt  = ec.get("datetime", "?")
        entry_line = (
            f"\n🕯 Entry Candle  : {col}({cno}) {cdt}"
        )

    return (
        f"🟢 <b>EMA 9 WAVE SIGNAL — BULLISH SETUP</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Symbol   : <a href='{link}'>{symbol}</a>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Point(0) : {sig['p0_dt']}  Low  ₹{sig['p0_val']:.2f}\n"
        f"📍 Point(1) : {sig['p1_dt']}  High ₹{sig['p1_val']:.2f}\n"
        f"📍 Point(2) : {sig['p2_dt']}  Low  ₹{sig['p2_val']:.2f}\n"
        f"📍 Point(3) : {sig['p3_dt']}  High ₹{sig['p3_val']:.2f}\n"
        f"📍 Point(4) : {sig['p4_dt']}  Low  ₹{sig['p4_val']:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📐 Fib −0.236 Level : ₹{sig['fib_ext_price']:.2f}"
        f"{entry_line}"
    )


def send_signal(sig: dict) -> bool:
    """Convenience wrapper — formats and sends a wave signal dict."""
    return send_alert(format_signal_message(sig))


def send_startup_message() -> bool:
    """Sends a startup ping so you know the scanner is live."""
    msg = (
        "🚀 <b>EMA 9 Wave Scanner STARTED</b>\n"
        "Strategy : EMA 9 Wave (−0.236 Level)\n"
        "Live data from Fyers · All indicators calculated fresh each run ✅"
    )
    return send_alert(msg)