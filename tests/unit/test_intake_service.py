"""Unit tests for request intake: validation, receipts, duplicates, and account isolation."""

from dataclasses import replace
from decimal import Decimal

import pytest

from binance_connector.domain.lifecycle import PositionState, ProcessingState, RequestStatus
from binance_connector.errors import MarketDataUnavailableError, UnknownSymbolError
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.market_data import ExchangeRules
from binance_connector.persistence.positions import PositionRepository
from binance_connector.persistence.requests import RequestRepository
from binance_connector.security.redaction import SecretRedactor
from binance_connector.services.intake import IntakeService

BTCUSDT_RULES = ExchangeRules(
    symbol="BTCUSDT", tick_size=Decimal("0.10"), step_size=Decimal("0.0001"), min_quantity=Decimal("0.0001"),
    market_step_size=Decimal("0.0001"), market_min_quantity=Decimal("0.0001"),
    market_max_quantity=Decimal("120"), min_notional=Decimal("50"),
)
MARK_PRICE = Decimal("75000")


class FakeRules:
    def __init__(self) -> None:
        self.unavailable = False

    def rules(self, symbol):
        if self.unavailable:
            raise MarketDataUnavailableError("exchangeInfo timed out")
        if symbol != "BTCUSDT":
            raise UnknownSymbolError(symbol)
        return BTCUSDT_RULES


class FakePrices:
    def __init__(self) -> None:
        self.mark_price = MARK_PRICE
        self.calls = 0

    def price(self, symbol):
        self.calls += 1
        return self.mark_price


def open_body(**overrides) -> dict:
    body = {
        "request_id": "request-1",
        "user_id": "user-1",
        "account_id": "account-1",
        "api_key": "demo-api-key-0001",
        "secret_key": "demo-api-secret-0001",
        "action": "OPEN",
        "timestamp": "2026-11-07T12:00:00Z",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "quantity": "0.002",
        "leverage": 5,
        "take_profit_price": "82000.0",
        "stop_loss_price": "74000.0",
    }
    body.update(overrides)
    return {key: value for key, value in body.items() if value is not None}


def close_body(**overrides) -> dict:
    body = {key: value for key, value in open_body().items()
            if key in ("user_id", "account_id", "api_key", "secret_key", "timestamp")}
    body.update(request_id="request-close-1", action="CLOSE", symbol="BTCUSDT")
    body.update(overrides)
    return {key: value for key, value in body.items() if value is not None}


@pytest.fixture
def repositories(session_factory, cipher):
    return (
        AccountRepository(session_factory, cipher=cipher, redactor=SecretRedactor()),
        RequestRepository(session_factory),
        PositionRepository(session_factory),
    )


@pytest.fixture
def rules() -> FakeRules:
    return FakeRules()


@pytest.fixture
def prices() -> FakePrices:
    return FakePrices()


@pytest.fixture
def intake(repositories, rules, prices, settings) -> IntakeService:
    return make_intake(repositories, rules, prices, settings)


def make_intake(repositories, rules, prices, settings, position_ids=None):
    accounts, requests, positions = repositories
    generated_ids = iter(position_ids or [f"p{index:023d}" for index in range(1, 100)])
    return IntakeService(accounts=accounts, requests=requests, positions=positions, rules=rules, prices=prices,
                         settings=settings, generate_position_id=lambda: next(generated_ids))


# --- accepted OPEN ------------------------------------------------------------------------------


def test_a_valid_open_is_accepted_with_a_position_and_queued(intake, repositories):
    outcome = intake.submit(open_body())

    _accounts, requests, positions = repositories
    assert outcome.http_status == 202
    receipt = outcome.body
    assert receipt["status"] == "ACCEPTED"
    assert receipt["request_id"] == "request-1"
    assert receipt["position_id"] == "p00000000000000000000001"
    assert receipt["reasons"] == []
    stored_request = requests.get("request-1")
    assert stored_request.processing_state is ProcessingState.PENDING
    position = positions.get(receipt["position_id"])
    assert position.state is PositionState.OPENING
    assert position.quantity == Decimal("0.002")
    assert position.take_profit_price == Decimal("82000.0")


def test_credentials_are_never_stored_with_the_request(intake, repositories):
    intake.submit(open_body())

    stored_payload = repositories[1].get("request-1").payload
    assert "api_key" not in stored_payload and "secret_key" not in stored_payload
    assert "demo-api-secret-0001" not in str(stored_payload)


def test_the_account_is_created_with_encrypted_credentials(intake, repositories):
    intake.submit(open_body())

    assert repositories[0].credentials("account-1").api_secret == "demo-api-secret-0001"


@pytest.mark.parametrize(
    ("quantity_fields", "expected_quantity"),
    [
        ({"quantity": "0.002"}, "0.002"),
        ({"quantity": "150", "quantity_type": "NOTIONAL"}, "0.002"),
        ({"quantity": "30", "quantity_type": "MARGIN", "leverage": 5}, "0.002"),
    ],
)
def test_all_three_quantity_types_are_converted_to_coins(intake, repositories, quantity_fields, expected_quantity):
    receipt = intake.submit(open_body(**quantity_fields)).body

    position = repositories[2].get(receipt["position_id"])
    assert position.quantity == Decimal(expected_quantity)
    assert position.requested_quantity == Decimal(quantity_fields["quantity"])


def test_a_limit_entry_is_sized_with_its_own_price(intake, repositories, prices):
    receipt = intake.submit(open_body(entry_type="LIMIT", entry_price="60000.0", quantity="120",
                                      quantity_type="NOTIONAL", take_profit_price="65000.0",
                                      stop_loss_price="58000.0")).body

    position = repositories[2].get(receipt["position_id"])
    assert receipt["status"] == "ACCEPTED"
    assert position.entry_type == "LIMIT"
    assert position.quantity == Decimal("0.002")  # 120 / 60000, not / the mark price
    assert prices.calls == 0


def test_a_market_entry_price_is_kept_as_information(intake, repositories):
    receipt = intake.submit(open_body(entry_price="74950.0")).body

    assert repositories[2].get(receipt["position_id"]).entry_price == Decimal("74950.0")


def test_two_users_can_open_the_same_symbol_at_once(intake):
    first = intake.submit(open_body())
    second = intake.submit(open_body(request_id="request-2", user_id="user-2", account_id="account-2",
                                     api_key="demo-api-key-0002", secret_key="demo-api-secret-0002"))

    assert (first.body["status"], second.body["status"]) == ("ACCEPTED", "ACCEPTED")
    assert first.body["position_id"] != second.body["position_id"]


# --- rejected OPEN ------------------------------------------------------------------------------


def assert_rejected(outcome, reason_fragment):
    assert outcome.http_status == 422
    assert outcome.body["status"] == "REJECTED"
    assert any(reason_fragment in reason for reason in outcome.body["reasons"]), outcome.body["reasons"]


@pytest.mark.parametrize(
    ("overrides", "reason_fragment"),
    [
        ({"symbol": "DOGEUSDT"}, "DOGEUSDT"),
        ({"quantity": "0.0005"}, "minimum notional"),
        ({"quantity": "0.00025"}, "multiple of 0.0001"),
        ({"quantity_type": "MARGIN", "leverage": None, "quantity": "30"}, "leverage"),
        ({"entry_type": "LIMIT"}, "entry_price"),
        ({"entry_type": "LIMIT", "entry_price": "60000.05"}, "tick size"),
        ({"take_profit_price": "74000.0", "stop_loss_price": "82000.0"}, "take_profit_price"),
        ({"take_profit_price": "74500.0", "stop_loss_price": None}, "take_profit_price"),
        ({"stop_loss_price": "76000.0", "take_profit_price": None}, "stop_loss_price"),
        ({"take_profit_price": "82000.05"}, "tick size"),
        ({"direction": "UP"}, "direction"),
        ({"quantity": "-1"}, "quantity"),
        ({"leverage": 200}, "leverage"),
        ({"unexpected_field": "x"}, "unexpected_field"),
    ],
)
def test_invalid_opens_are_rejected_with_reasons(intake, repositories, overrides, reason_fragment):
    outcome = intake.submit(open_body(**overrides))

    assert_rejected(outcome, reason_fragment)
    stored_request = repositories[1].get("request-1")
    assert stored_request.status is RequestStatus.REJECTED
    assert stored_request.processing_state is None
    assert repositories[2].list_active() == []


def test_the_price_guard_rejects_a_market_entry_too_far_from_the_mark_price(repositories, rules, prices, settings):
    guarded = make_intake(repositories, rules, prices, replace(settings, price_guard_max_deviation_percent=Decimal("1")))

    outcome = guarded.submit(open_body(entry_price="73000.0"))  # 2.7% below 75000

    assert_rejected(outcome, "price guard")


def test_the_price_guard_lets_a_close_enough_entry_through(repositories, rules, prices, settings):
    guarded = make_intake(repositories, rules, prices, replace(settings, price_guard_max_deviation_percent=Decimal("1")))

    assert guarded.submit(open_body(entry_price="74500.0")).body["status"] == "ACCEPTED"  # 0.67%


def test_a_second_open_on_the_same_symbol_and_account_is_rejected(intake):
    intake.submit(open_body())

    outcome = intake.submit(open_body(request_id="request-2"))

    assert_rejected(outcome, "already has an active position")


def test_an_account_claimed_by_another_user_is_rejected(intake, repositories):
    intake.submit(open_body())

    outcome = intake.submit(open_body(request_id="request-2", user_id="user-2"))

    assert_rejected(outcome, "belongs to a different user")
    assert repositories[0].get("account-1").user_id == "user-1"


# --- envelope errors and outages: nothing stored ------------------------------------------------


@pytest.mark.parametrize("missing_field", ["request_id", "user_id", "account_id", "api_key", "secret_key", "action"])
def test_a_request_missing_its_identity_is_a_400_and_is_not_stored(intake, repositories, missing_field):
    outcome = intake.submit(open_body(**{missing_field: None}))

    assert outcome.http_status == 400
    assert outcome.body["error"] == "validation_error"
    assert missing_field in [detail["field"] for detail in outcome.body["details"]]


def test_market_data_being_unavailable_is_a_503_and_nothing_is_stored(intake, repositories, rules):
    rules.unavailable = True

    outcome = intake.submit(open_body())

    assert outcome.http_status == 503
    assert outcome.body["error"] == "market_data_unavailable"
    assert repositories[1].find("request-1") is None


# --- duplicates (option 2) ----------------------------------------------------------------------


def test_the_same_request_again_returns_the_original_receipt_without_a_second_position(intake, repositories):
    original = intake.submit(open_body())

    repeated = intake.submit(open_body())

    assert repeated.http_status == 200
    assert repeated.body == {**original.body, "duplicate": True}
    assert len(repositories[2].list_active()) == 1


def test_a_repeated_rejection_is_returned_as_it_was(intake):
    original = intake.submit(open_body(symbol="DOGEUSDT"))

    repeated = intake.submit(open_body(symbol="DOGEUSDT"))

    assert repeated.http_status == 200
    assert repeated.body["status"] == "REJECTED"
    assert repeated.body["reasons"] == original.body["reasons"]


def test_new_credentials_alone_do_not_make_a_request_different(intake):
    intake.submit(open_body())

    repeated = intake.submit(open_body(api_key="rotated-key-0002", secret_key="rotated-secret-0002"))

    assert repeated.http_status == 200


def test_the_same_request_id_with_a_different_body_is_a_409(intake):
    intake.submit(open_body())

    outcome = intake.submit(open_body(quantity="0.003"))

    assert outcome.http_status == 409
    assert outcome.body["error"] == "request_id_conflict"


# --- CLOSE --------------------------------------------------------------------------------------


@pytest.mark.parametrize("target", [{"symbol": "BTCUSDT"}, {"position_id": "p00000000000000000000001",
                                                            "symbol": None}])
def test_a_close_for_an_open_position_is_accepted_and_queued(intake, repositories, target):
    intake.submit(open_body())

    outcome = intake.submit(close_body(**target))

    assert outcome.http_status == 202
    assert outcome.body["status"] == "ACCEPTED"
    assert outcome.body["position_id"] == "p00000000000000000000001"
    assert repositories[1].get("request-close-1").processing_state is ProcessingState.PENDING


def test_a_close_without_a_matching_position_is_rejected(intake):
    assert_rejected(intake.submit(close_body()), "no active position")


def test_a_close_needs_a_position_id_or_a_symbol(intake):
    assert_rejected(intake.submit(close_body(symbol=None)), "position_id or symbol")


def test_a_close_cannot_reach_another_accounts_position(intake):
    intake.submit(open_body())

    outcome = intake.submit(close_body(position_id="p00000000000000000000001", symbol=None,
                                       account_id="account-2", user_id="user-2",
                                       api_key="demo-api-key-0002", secret_key="demo-api-secret-0002"))

    assert_rejected(outcome, "not found for account account-2")


def test_a_close_for_a_finished_position_returns_its_final_result(intake, repositories):
    intake.submit(open_body())
    repositories[2].transition("p00000000000000000000001", PositionState.FAILED, last_error="entry rejected")

    outcome = intake.submit(close_body(position_id="p00000000000000000000001", symbol=None))

    assert outcome.http_status == 200
    assert outcome.body["status"] == "ACCEPTED"
    assert outcome.body["already_closed"] is True
    assert outcome.body["position"]["state"] == "FAILED"
    assert repositories[1].get("request-close-1").processing_state is ProcessingState.DONE
