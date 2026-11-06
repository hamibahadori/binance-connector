"""Helpers shared by the execution tests (position manager, execution manager, reconciliation)."""

from decimal import Decimal

from binance_connector.persistence.positions import NewPosition, Position, PositionRepository
from tests.fakes import FakeBinance

POSITION_ID = "p00000000000000000000001"
ACCOUNT_IDS = ("account-1", "account-2")


class FakeClients:
    """One simulated Binance account per account_id (stands in for AccountClients)."""

    def __init__(self, fill_price: str = "78000.0") -> None:
        self.fill_price = fill_price
        self.by_account: dict[str, FakeBinance] = {}

    def for_account(self, account_id: str) -> FakeBinance:
        return self.by_account.setdefault(account_id, FakeBinance(fill_price=self.fill_price))


def store_position(positions: PositionRepository, position_id: str = POSITION_ID, account_id: str = "account-1",
                   **overrides) -> Position:
    """Store a position as intake would (OPENING); a 0.002 BTCUSDT long with TP/SL by default."""
    fields = {
        "position_id": position_id, "account_id": account_id, "symbol": "BTCUSDT", "direction": "LONG",
        "quantity": Decimal("0.002"), "requested_quantity": Decimal("0.002"), "quantity_type": "BASE",
        "leverage": 5, "entry_type": "MARKET", "entry_price": None,
        "take_profit_price": Decimal("82000.0"), "stop_loss_price": Decimal("74000.0"),
        "open_request_id": f"request-for-{position_id}",
    }
    fields.update(overrides)
    return positions.create(NewPosition(**fields))
