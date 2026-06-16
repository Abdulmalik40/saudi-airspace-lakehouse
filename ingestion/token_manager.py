import requests
from datetime import datetime, timedelta, timezone
import os 
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import ConnectionError, Timeout, HTTPError
from typing import Optional
logger = logging.getLogger(__name__)

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

try:
    CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
except KeyError:
    raise KeyError("OPENSKY_CLIENT_ID environment variable is not set")   


try:
    CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]
except KeyError:
    raise KeyError("OPENSKY_CLIENT_SECRET environment variable is not set")

# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30


class TokenManager:
    def __init__(self) -> None:
       self.token: Optional[str] = None
       self.expires_at: Optional[datetime] = None

    def get_token(self)-> str:
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now(timezone.utc) < self.expires_at:
            return self.token
        return self._refresh()
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, Timeout)))
    def _refresh(self) -> str:
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=(5,10)
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        
        logger.info("Refreshed OpenSky token, expires at %s", self.expires_at.isoformat())
        
        return self.token

    def headers(self) -> dict:
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}




