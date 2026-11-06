"""Exception types raised inside binance-connector.

The API layer turns each of these into one HTTP status code (see
``binance_connector.api.error_handlers``), so every failure a caller can see
has exactly one type here.
"""

from decimal import Decimal


class BinanceConnectorError(Exception):
    """Base class for every error this service raises on purpose."""


class ConfigurationError(BinanceConnectorError):
    """A required setting is missing or invalid; the service cannot start."""


class BinanceError(BinanceConnectorError):
    """Base class for failures talking to Binance."""


class BinanceAPIError(BinanceError):
    """Binance received the request and rejected it (HTTP 4xx).

    Binance error bodies look like ``{"code": -2019, "msg": "Margin is insufficient."}``.
    ``code`` is ``None`` when the body was not in that shape.
    """

    def __init__(self, http_status: int, code: int | None, message: str) -> None:
        super().__init__(f"Binance rejected the request (HTTP {http_status}, code {code}): {message}")
        self.http_status = http_status
        self.code = code
        self.message = message


class BinanceUnavailableError(BinanceError):
    """The request never reached Binance, so nothing can have been executed."""


class BinanceUnknownOutcomeError(BinanceError):
    """Binance may or may not have executed the request.

    Raised on a 5xx reply or a network failure after connecting. Binance documents that
    the execution status is then unknown and the order may have gone through, so callers
    must check (for example by ``client_order_id``) before trying again.
    """

    def __init__(self, message: str, client_order_id: str | None = None) -> None:
        super().__init__(message)
        self.client_order_id = client_order_id


class NoOpenPositionError(BinanceConnectorError):
    """A close was requested for a symbol that has no open position."""

    def __init__(self, symbol: str) -> None:
        super().__init__(f"There is no open position for {symbol}.")
        self.symbol = symbol


class HedgeModeNotSupportedError(BinanceConnectorError):
    """The account holds hedge-mode (LONG/SHORT) positions; only one-way mode is supported."""

    def __init__(self, symbol: str) -> None:
        super().__init__(
            f"The account holds hedge-mode positions for {symbol}. "
            "This service supports one-way position mode only."
        )
        self.symbol = symbol


class InvalidTransitionError(BinanceConnectorError):
    """A request or position was asked to move to a state its current state does not allow.

    Usually means another thread or process changed it first.
    """

    def __init__(self, current_state: object, target_state: object) -> None:
        super().__init__(f"Cannot move from {current_state} to {target_state}.")
        self.current_state = current_state
        self.target_state = target_state


class CredentialDecryptionError(BinanceConnectorError):
    """Stored credentials could not be decrypted (wrong CREDENTIALS_ENCRYPTION_KEY, or tampered data)."""


class AccountNotFoundError(BinanceConnectorError):
    def __init__(self, account_id: str) -> None:
        super().__init__(f"There is no account {account_id}.")
        self.account_id = account_id


class AccountOwnershipError(BinanceConnectorError):
    """A request named an account that belongs to a different user."""

    def __init__(self, account_id: str) -> None:
        super().__init__(f"Account {account_id} belongs to a different user.")
        self.account_id = account_id


class RequestNotFoundError(BinanceConnectorError):
    def __init__(self, request_id: str) -> None:
        super().__init__(f"There is no request {request_id}.")
        self.request_id = request_id


class DuplicateRequestError(BinanceConnectorError):
    """A request_id was used before. ``existing`` is the stored request."""

    def __init__(self, existing: object) -> None:
        super().__init__(f"Request {getattr(existing, 'request_id', '?')} was already received.")
        self.existing = existing


class PositionNotFoundError(BinanceConnectorError):
    def __init__(self, position_id: str) -> None:
        super().__init__(f"There is no position {position_id}.")
        self.position_id = position_id


class PositionConflictError(BinanceConnectorError):
    """The account already has an active position on the symbol (one per symbol per account)."""

    def __init__(self, account_id: str, symbol: str) -> None:
        super().__init__(f"Account {account_id} already has an active position on {symbol}.")
        self.account_id = account_id
        self.symbol = symbol


class CloseQuantityExceedsPositionError(BinanceConnectorError):
    """A partial close asked for more than the open position holds."""

    def __init__(self, symbol: str, requested_quantity: Decimal, open_quantity: Decimal) -> None:
        super().__init__(
            f"Cannot close {requested_quantity} of {symbol}: "
            f"the open position is only {open_quantity}."
        )
        self.symbol = symbol
        self.requested_quantity = requested_quantity
        self.open_quantity = open_quantity
