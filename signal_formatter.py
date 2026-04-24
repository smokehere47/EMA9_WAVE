# ================= SIGNAL FORMATTER — EMA 9 WAVE STRATEGY =================
#
# Formats signal dicts into the required terminal output format:
#
#   Symbol: NSE:RELIANCE-EQ
#   Point(0): 2026-04-24 09:18, Low(2780.50)
#   Point(1): 2026-04-24 09:27, High(2810.20)
#   Point(2): 2026-04-24 09:36, Low(2791.40)
#   Point(3): 2026-04-24 09:48, High(2825.60)
#   Point(4): 2026-04-24 09:54, Low(2800.10)
#   Note: entry candle: max within next 3 candle close or open of candle
#   Entry Candle: Green(2) 2026-04-24 09:57
# ===========================================================================

DIVIDER = "─" * 55


def format_signal_terminal(sig: dict) -> str:
    """
    Render a single signal dict as the required terminal output block.
    """
    lines = [
        DIVIDER,
        f"  Symbol: {sig['symbol']}",
        f"  Point(0): {sig['p0_dt']},  Low({sig['p0_val']:.2f})",
        f"  Point(1): {sig['p1_dt']},  High({sig['p1_val']:.2f})",
        f"  Point(2): {sig['p2_dt']},  Low({sig['p2_val']:.2f})",
        f"  Point(3): {sig['p3_dt']},  High({sig['p3_val']:.2f})",
        f"  Point(4): {sig['p4_dt']},  Low({sig['p4_val']:.2f})",
        f"  Fib −0.236 Level: ₹{sig['fib_ext_price']:.2f}",
    ]

    ec = sig.get("entry_candle")
    if ec:
        lines.append(
            f"  Note: entry candle: max within next 3 candle close or open of candle"
        )
        lines.append(
            f"  Entry Candle: {ec['color']}({ec['candle_no']}) {ec['datetime']}"
        )
    else:
        lines.append("  Entry Candle: Pending (ENABLE_ENTRY = False)")

    lines.append(DIVIDER)
    return "\n".join(lines)


def print_signal(sig: dict) -> None:
    print(format_signal_terminal(sig))


def print_scan_summary(
    total_symbols: int,
    signals_found: int,
    scan_secs: float,
    target_date,
    mode: str,
) -> None:
    print(f"\n  {'=' * 55}")
    print(f"  EMA 9 WAVE SCAN COMPLETE")
    print(f"  {'─' * 55}")
    print(f"  Date     : {target_date}  [{mode}]")
    print(f"  Symbols  : {total_symbols}")
    print(f"  Signals  : {signals_found}")
    print(f"  Duration : {scan_secs:.1f}s")
    print(f"  {'=' * 55}\n")