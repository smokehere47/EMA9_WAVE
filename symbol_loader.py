# ================= SYMBOL LOADER — EMA 9 WAVE STRATEGY =================
#
# DEV_MODE = True  → returns DEV_SYMBOLS from config (fast testing)
# DEV_MODE = False → loads all symbols from INPUT_EXCEL (production)
#
# To scale from dev to production: change DEV_MODE = False in config.py.
# No other code changes required.
# ========================================================================

import pandas as pd
from config import DEV_MODE, DEV_SYMBOLS, INPUT_EXCEL, SYMBOL_COLUMN


def load_symbols() -> list[str]:
    if DEV_MODE:
        print(f"  [DEV MODE] Using {len(DEV_SYMBOLS)} hardcoded symbol(s).")
        return list(DEV_SYMBOLS)

    df   = pd.read_excel(INPUT_EXCEL)
    syms = df[SYMBOL_COLUMN].dropna().astype(str).str.strip().tolist()
    syms = [s for s in syms if s]
    print(f"  [PROD MODE] Loaded {len(syms)} symbol(s) from {INPUT_EXCEL}")
    return syms