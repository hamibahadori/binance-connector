"""HMAC-SHA256 signing for Binance signed endpoints.

Binance signs the exact parameter string that is sent, so the caller must
sign and send the same string, with the same parameter order and encoding.
Reference: https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
"""

import hashlib
import hmac


def sign_query(query_string: str, api_secret: str) -> str:
    """Return the hex-encoded HMAC-SHA256 of ``query_string`` keyed with ``api_secret``."""
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
