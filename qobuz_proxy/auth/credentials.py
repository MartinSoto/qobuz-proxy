"""
User token persistence.

Stores and retrieves OAuth tokens from a local cache file.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cache location
CACHE_DIR = Path(os.environ.get("QOBUZPROXY_DATA_DIR", Path.home() / ".qobuz-proxy"))
CACHE_FILE = CACHE_DIR / "credentials.json"


def load_user_token() -> Optional[dict[str, str]]:
    """Load user auth token from cache file."""
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                creds: dict[str, str] = json.load(f)
                if creds.get("user_id") and creds.get("user_auth_token"):
                    return {
                        "user_id": creds["user_id"],
                        "user_auth_token": creds["user_auth_token"],
                        "email": creds.get("email", ""),
                    }
    except Exception as e:
        logger.warning(f"Failed to load user token: {e}")
    return None


def save_user_token(user_id: str, auth_token: str, email: str) -> bool:
    """Save user auth token to cache file."""
    try:
        existing: dict[str, str] = {}
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                existing = json.load(f)
        existing["user_id"] = user_id
        existing["user_auth_token"] = auth_token
        existing["email"] = email
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(existing, f, indent=2)
        logger.info("Saved user token to cache")
        return True
    except Exception as e:
        logger.error(f"Failed to save user token: {e}")
        return False


def clear_user_token() -> bool:
    """Remove user auth token from cache file."""
    try:
        if not CACHE_FILE.exists():
            return True
        with open(CACHE_FILE) as f:
            existing: dict[str, str] = json.load(f)
        for key in ("user_id", "user_auth_token", "email"):
            existing.pop(key, None)
        with open(CACHE_FILE, "w") as f:
            json.dump(existing, f, indent=2)
        logger.info("Cleared user token from cache")
        return True
    except Exception as e:
        logger.error(f"Failed to clear user token: {e}")
        return False
