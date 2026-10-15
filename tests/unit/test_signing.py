"""Unit tests for HMAC-SHA256 request signing."""

import string

from binance_connector.binance.signing import sign_query

# The worked example from Binance's USD-M Futures "General Info" page:
# https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
BINANCE_DOC_SECRET = "2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9"
BINANCE_DOC_PAYLOAD = (
    "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000"
    "&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943"
)
BINANCE_DOC_SIGNATURE = "3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9"


def test_signature_matches_binance_documented_example():
    assert sign_query(BINANCE_DOC_PAYLOAD, BINANCE_DOC_SECRET) == BINANCE_DOC_SIGNATURE


def test_signature_changes_when_payload_changes():
    altered_payload = BINANCE_DOC_PAYLOAD.replace("quantity=1", "quantity=2")

    assert sign_query(altered_payload, BINANCE_DOC_SECRET) != BINANCE_DOC_SIGNATURE


def test_signature_is_64_lowercase_hex_characters():
    signature = sign_query("symbol=BTCUSDT", "any-secret")

    assert len(signature) == 64
    assert set(signature) <= set(string.hexdigits.lower())
