import os
import logging
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from ingestion.token_manager import TokenManager

logger = logging.getLogger(__name__)

STATES_URL = "https://opensky-network.org/api/states/all"

try:
    BBOX_LAMIN = float(os.environ["BBOX_LAMIN"])
except (KeyError, ValueError):
    logger.error("Invalid or missing BBOX_LAMIN environment variable")
    raise
try:
    BBOX_LAMAX = float(os.environ["BBOX_LAMAX"])
except (KeyError, ValueError):
    logger.error("Invalid or missing BBOX_LAMAX environment variable")
    raise
try:
    BBOX_LOMIN = float(os.environ["BBOX_LOMIN"])
except (KeyError, ValueError):
    logger.error("Invalid or missing BBOX_LOMIN environment variable")
    raise
try:
    BBOX_LOMAX = float(os.environ["BBOX_LOMAX"])
except (KeyError, ValueError):
    logger.error("Invalid or missing BBOX_LOMAX environment variable")
    raise


_token_manager = TokenManager()


def _is_retryable(exc: BaseException) -> bool:
    """Return True if the exception is a transient error worth retrying."""

    if isinstance(exc, (ConnectionError, Timeout)):
        return True
    if isinstance(exc, HTTPError) and exc.response is not None:
        status = exc.response.status_code
        if status >= 500 or status == 429:
            return True
    return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
)
def fetch_states() -> dict:
    """Fetch current aircraft states over the MENA bbox from OpenSky.
    
    Returns the raw JSON response as a dict. Does not parse or validate
    the state vectors — that is the silver layer's job.
    """
    params = {
        "lamin": BBOX_LAMIN,
        "lamax": BBOX_LAMAX,
        "lomin": BBOX_LOMIN,
        "lomax": BBOX_LOMAX,
    }
    headers = _token_manager.headers()
    response = requests.get(STATES_URL, params=params, headers=headers, timeout=(5,30))
    response.raise_for_status()
    remaining = response.headers.get("X-Rate-Limit-Remaining")
    logger.info("OpenSky call succeeded, credits remaining: %s", remaining)
    return response.json()
