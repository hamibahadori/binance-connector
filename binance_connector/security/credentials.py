"""Account credentials, and their encryption at rest (decision D-55 / G).

Every account's Binance API key and secret arrive with Django's requests and are stored
encrypted with Fernet (AES-128-CBC with an HMAC-SHA256 check, from the ``cryptography``
package). The key comes from ``CREDENTIALS_ENCRYPTION_KEY``; generate one with::

    python -m binance_connector.security.credentials

Losing that key makes stored credentials unreadable; Django resends them with the next
request for each account, which re-encrypts them.
"""

import hashlib
from dataclasses import dataclass, field

from cryptography.fernet import Fernet, InvalidToken

from binance_connector.errors import ConfigurationError, CredentialDecryptionError

ENCRYPTION_KEY_VARIABLE = "CREDENTIALS_ENCRYPTION_KEY"


@dataclass(frozen=True)
class ApiCredentials:
    """One Binance account's API key and secret. Neither appears in ``repr``."""

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)


class CredentialCipher:
    """Encrypts and decrypts credential strings with one Fernet key."""

    def __init__(self, key: str) -> None:
        try:
            self._fernet = Fernet(key.strip().encode())
        except (ValueError, TypeError) as error:
            raise ConfigurationError(
                f"{ENCRYPTION_KEY_VARIABLE} is not a valid Fernet key. Generate one with "
                "`python -m binance_connector.security.credentials`."
            ) from error

    def encrypt(self, plain_text: str) -> str:
        return self._fernet.encrypt(plain_text.encode()).decode()

    def decrypt(self, token: str) -> str:
        try:
            return self._fernet.decrypt(token.encode()).decode()
        except InvalidToken as error:
            raise CredentialDecryptionError(
                f"Stored credentials could not be decrypted; was {ENCRYPTION_KEY_VARIABLE} changed?"
            ) from error


def fingerprint(value: str) -> str:
    """A short, non-reversible tag for a credential, to tell keys apart without showing them."""
    return hashlib.sha256(value.encode()).hexdigest()[:12]


if __name__ == "__main__":
    print(Fernet.generate_key().decode())
