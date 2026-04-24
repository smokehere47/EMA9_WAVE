# ================= FYERS CLIENT — EMA 9 WAVE STRATEGY =================
#
# Automatically refreshes the access token using the refresh token
# whenever the current token is expired or invalid.
# No browser login needed — fully automated.
# =======================================================================

import requests
from fyers_apiv3 import fyersModel
from config import (
    CLIENT_ID_FILE,
    ACCESS_TOKEN_FILE,
    REFRESH_TOKEN_FILE,
    FYERS_APP_ID_HASH,
    FYERS_PIN,
)

REFRESH_URL = "https://api-t1.fyers.in/api/v3/validate-refresh-token"


def _read_file(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        raise SystemExit(f"❌ Required file not found: {path}")


def _refresh_access_token() -> str:
    """
    Calls the Fyers refresh token API and saves the new access token to disk.
    Returns the new access token string.
    """
    print("🔄 Access token expired — refreshing automatically...")
    refresh_token = _read_file(REFRESH_TOKEN_FILE)
    client_id     = _read_file(CLIENT_ID_FILE)

    if not refresh_token:
        raise SystemExit(
            f"❌ Refresh token file is empty: {REFRESH_TOKEN_FILE}\n"
            "  Generate a new refresh token from the Fyers developer portal."
        )

    response = requests.post(
        REFRESH_URL,
        headers={"Content-Type": "application/json"},
        json={
            "grant_type":   "refresh_token",
            "appIdHash":    FYERS_APP_ID_HASH,
            "refresh_token": refresh_token,
            "pin":          FYERS_PIN,
        },
        timeout=15,
    )

    data         = response.json()
    access_token = data.get("access_token")

    if not access_token:
        raise SystemExit(
            f"❌ Token refresh failed | status: {response.status_code} | response: {data}\n"
            "  Check your FYERS_APP_ID_HASH and FYERS_PIN in config.py."
        )

    with open(ACCESS_TOKEN_FILE, "w") as f:
        f.write(access_token)

    print("✅ Access token refreshed and saved")
    return access_token


def _build_fyers(client_id: str, access_token: str):
    return fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )


def _is_token_valid(fyers) -> bool:
    """Returns True if the current token is valid."""
    try:
        profile = fyers.get_profile()
        return profile.get("s") == "ok"
    except Exception:
        return False


def get_fyers():
    """
    Loads credentials and returns an authenticated FyersModel instance.
    If the current access token is expired, automatically refreshes it
    using the refresh token — no manual steps required.
    """
    client_id    = _read_file(CLIENT_ID_FILE)
    access_token = _read_file(ACCESS_TOKEN_FILE)
    fyers        = _build_fyers(client_id, access_token)

    if not _is_token_valid(fyers):
        access_token = _refresh_access_token()
        fyers        = _build_fyers(client_id, access_token)

        if not _is_token_valid(fyers):
            raise SystemExit(
                "❌ Token refresh succeeded but new token is still invalid.\n"
                "  Your refresh token may have expired — regenerate it from "
                "the Fyers developer portal."
            )

    print("✅ Fyers connected and authenticated")
    return fyers


def check_token_mid_run(response: dict) -> bool:
    """
    Detects token expiry from a mid-run API response.
    Returns True if the token has expired so main.py can re-authenticate.
    Fyers returns code -16 or 'Access Denied' when the token is expired.
    """
    code = str(response.get("code", ""))
    msg  = str(response.get("message", "")).lower()
    return code in ("-16", "16") or "access denied" in msg or "invalid token" in msg