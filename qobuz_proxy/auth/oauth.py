"""Qobuz OAuth authentication.

Implements the OAuth flow used by the Qobuz desktop app:
1. Redirect user to Qobuz sign-in page with a callback URL
2. Qobuz redirects back with a ``code_autorisation`` query parameter
3. Exchange that code for a user auth token via the API
"""

import logging
from urllib.parse import parse_qs, urlencode, urlparse

import aiohttp

logger = logging.getLogger(__name__)

# Qobuz desktop app credentials
OAUTH_APP_ID = "304027809"
OAUTH_PRIVATE_KEY = "6lz8C03UDIC7"
OAUTH_APP_SECRET = "96c4538ca81015a5be0c1d5bd9573844"
QOBUZ_API_BASE = "https://www.qobuz.com/api.json/0.2"
QOBUZ_OAUTH_URL = "https://www.qobuz.com/signin/oauth"


def build_oauth_url(redirect_url: str) -> str:
    """Build the Qobuz OAuth URL that the user should be redirected to."""
    params = urlencode({"ext_app_id": OAUTH_APP_ID, "redirect_url": redirect_url})
    return f"{QOBUZ_OAUTH_URL}?{params}"


def extract_code(url: str) -> str:
    """Extract ``code_autorisation`` from a callback URL.

    Raises ValueError if the parameter is missing.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    codes = params.get("code_autorisation", [])
    if not codes:
        raise ValueError(f"No code_autorisation found in URL: {url}")
    return codes[0]


async def exchange_code(code: str, **_kwargs: object) -> dict[str, str]:
    """Exchange an OAuth authorization code for user credentials.

    Returns a dict with ``user_id``, ``user_auth_token``, ``display_name``,
    and ``email`` keys.
    """

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Step 1: exchange code for token (always uses OAuth app credentials)
        async with session.get(
            f"{QOBUZ_API_BASE}/oauth/callback",
            params={"code": code, "private_key": OAUTH_PRIVATE_KEY},
            headers={"X-App-Id": OAUTH_APP_ID},
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"Token exchange failed ({resp.status}): {text}")
            data = await resp.json()

        token = data["token"]
        user_id = str(data["user_id"])

        # Step 2: validate token and fetch profile
        async with session.post(
            f"{QOBUZ_API_BASE}/user/login",
            data="extra=partner",
            headers={
                "X-App-Id": OAUTH_APP_ID,
                "X-User-Auth-Token": token,
                "Content-Type": "text/plain;charset=UTF-8",
            },
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Login validation failed: {resp.status}")
            profile_data = await resp.json()

    user = profile_data.get("user", {})
    avatar = user.get("avatar", "")
    if isinstance(avatar, dict):
        avatar = avatar.get("url", "") or avatar.get("large", "") or ""

    return {
        "user_id": user_id,
        "user_auth_token": token,
        "display_name": user.get("display_name", ""),
        "email": user.get("email", ""),
        "avatar": avatar,
    }
