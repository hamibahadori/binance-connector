"""One Binance client per account, built from that account's (decrypted) credentials.

Clients are cached per account and rebuilt when the account's credentials change (the
latest request's credentials win, decision I), detected by the key fingerprint.
Thread-safe: execution threads and stream threads share one cache.
"""

import threading
from collections.abc import Callable

from binance_connector.binance.client import BinanceFuturesClient
from binance_connector.config import Settings
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.security.credentials import ApiCredentials

ClientFactory = Callable[[Settings, ApiCredentials], BinanceFuturesClient]


class AccountClients:
    def __init__(
        self,
        *,
        accounts: AccountRepository,
        settings: Settings,
        client_factory: ClientFactory = BinanceFuturesClient,
    ) -> None:
        self._accounts = accounts
        self._settings = settings
        self._client_factory = client_factory
        self._cache: dict[str, tuple[str, BinanceFuturesClient]] = {}
        self._lock = threading.Lock()

    def for_account(self, account_id: str) -> BinanceFuturesClient:
        """The account's client. Raises AccountNotFoundError for an unknown account."""
        account = self._accounts.get(account_id)
        with self._lock:
            cached = self._cache.get(account_id)
            if cached is not None and cached[0] == account.api_key_fingerprint:
                return cached[1]
        client = self._client_factory(self._settings, self._accounts.credentials(account_id))
        with self._lock:
            self._cache[account_id] = (account.api_key_fingerprint, client)
        return client
