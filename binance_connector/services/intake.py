"""Intake of Django's requests (spec §3, §4, §8): validate, store, answer at once.

Nothing here talks to an account on Binance: execution happens in the worker. Intake only
reads public market data (rules and mark price) to validate and size the order.

Order of work for one request:

1. **Envelope** (ids, credentials, action, timestamp). Invalid → 400, nothing stored.
2. **Duplicates** (decision D-56, option 2). A known request_id with the same body (minus
   credentials) returns the stored receipt with ``duplicate: true`` and executes nothing;
   with a different body → 409.
3. **Account**: created, or its owner confirmed and its credentials refreshed (encrypted).
   An account_id owned by another user is a rejection (spec §4: users never mix).
4. **Market inputs** for an OPEN: exchange rules, and the mark price for a MARKET entry.
   If Binance cannot be reached → 503 and nothing is stored, so Django can simply retry.
5. **Store** the request (RECEIVED → VALIDATING), **decide** (ACCEPTED | REJECTED), and
   store the receipt as the request's response, so a duplicate gets exactly the same answer.

ACCEPTED OPENs create their position (OPENING) at once, so the receipt carries its
position_id. ACCEPTED requests are queued for the worker.
"""

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from pydantic import ValidationError

from binance_connector.api.request_schemas import (
    ClosePayload,
    OpenPayload,
    RequestEnvelope,
    payload_fields,
    without_secrets,
)
from binance_connector.config import Settings
from binance_connector.domain.lifecycle import RequestAction, RequestStatus, is_active
from binance_connector.domain.sizing import EntryType, SizingError, is_multiple, plain, size_order
from binance_connector.errors import (
    AccountOwnershipError,
    DuplicateRequestError,
    MarketDataUnavailableError,
    PositionConflictError,
    PositionNotFoundError,
    UnknownSymbolError,
)
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.market_data import ExchangeRules
from binance_connector.persistence.positions import NewPosition, Position, PositionRepository
from binance_connector.persistence.requests import NewRequest, Request, RequestRepository
from binance_connector.persistence.writes import utc_now
from binance_connector.presentation import receipt
from binance_connector.security.credentials import ApiCredentials


class RulesSource(Protocol):
    def rules(self, symbol: str) -> ExchangeRules: ...


class PriceSource(Protocol):
    def price(self, symbol: str) -> Decimal: ...


@dataclass(frozen=True)
class IntakeOutcome:
    http_status: int
    body: dict[str, Any]


@dataclass(frozen=True)
class _MarketInputs:
    rules: ExchangeRules | None = None
    reference_price: Decimal | None = None
    mark_price: Decimal | None = None
    rejection: str | None = None


def new_position_id() -> str:
    """24 hex characters: unique, and short enough for ``lx-<id>-tp`` to fit Binance's 36."""
    return uuid.uuid4().hex[:24]


def payload_hash(body: dict[str, Any]) -> str:
    """Fingerprint of a request body without its credentials (they may rotate between retries)."""
    return hashlib.sha256(json.dumps(without_secrets(body), sort_keys=True, default=str).encode()).hexdigest()


class IntakeService:
    def __init__(
        self,
        *,
        accounts: AccountRepository,
        requests: RequestRepository,
        positions: PositionRepository,
        rules: RulesSource,
        prices: PriceSource,
        settings: Settings,
        generate_position_id: Callable[[], str] = new_position_id,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._accounts = accounts
        self._requests = requests
        self._positions = positions
        self._rules = rules
        self._prices = prices
        self._settings = settings
        self._generate_position_id = generate_position_id
        self._clock = clock

    def submit(self, body: dict[str, Any]) -> IntakeOutcome:
        try:
            envelope = RequestEnvelope.model_validate(body)
        except ValidationError as error:
            return IntakeOutcome(400, {"error": "validation_error", "message": "The request cannot be identified.",
                                       "details": _details(error)})

        body_hash = payload_hash(body)
        existing = self._requests.find(envelope.request_id)
        if existing is not None:
            return self._duplicate(existing, body_hash)

        ownership_problem = None
        try:
            self._accounts.upsert(envelope.account_id, envelope.user_id,
                                  ApiCredentials(envelope.api_key, envelope.secret_key))
        except AccountOwnershipError as error:
            ownership_problem = str(error)

        payload, payload_problems = _parse_payload(envelope.action, payload_fields(body))
        market_inputs = _MarketInputs()
        if isinstance(payload, OpenPayload) and not payload_problems and ownership_problem is None:
            try:
                market_inputs = self._market_inputs(payload)
            except MarketDataUnavailableError as error:
                return IntakeOutcome(503, {"error": "market_data_unavailable", "message": f"{error} Retry later "
                                           "with the same request_id; nothing was stored."})

        try:
            request = self._requests.create(NewRequest(
                request_id=envelope.request_id,
                account_id=envelope.account_id,
                user_id=envelope.user_id,
                action=envelope.action,
                payload=without_secrets(body),
                payload_hash=body_hash,
                requested_at=envelope.timestamp,
            ))
        except DuplicateRequestError as duplicate:
            return self._duplicate(duplicate.existing, body_hash)
        self._requests.transition(request.request_id, RequestStatus.VALIDATING)

        reasons = list(payload_problems)
        if ownership_problem:
            reasons.append(ownership_problem)
        if reasons:
            return self._reject(request, reasons)
        if isinstance(payload, OpenPayload):
            return self._decide_open(request, payload, market_inputs)
        return self._decide_close(request, payload)

    # --- OPEN ---------------------------------------------------------------------------------

    def _market_inputs(self, payload: OpenPayload) -> _MarketInputs:
        try:
            rules = self._rules.rules(payload.symbol)
        except UnknownSymbolError as error:
            return _MarketInputs(rejection=str(error))
        mark_price = None
        if payload.entry_type is EntryType.LIMIT:
            reference_price = payload.entry_price
        else:
            mark_price = self._prices.price(payload.symbol)
            reference_price = mark_price
        return _MarketInputs(rules=rules, reference_price=reference_price, mark_price=mark_price)

    def _decide_open(self, request: Request, payload: OpenPayload, inputs: _MarketInputs) -> IntakeOutcome:
        if inputs.rejection:
            return self._reject(request, [inputs.rejection])
        rules = inputs.rules
        reasons = []
        for field_name in ("entry_price", "take_profit_price", "stop_loss_price"):
            price = getattr(payload, field_name)
            if price is not None and (field_name != "entry_price" or payload.entry_type is EntryType.LIMIT):
                if not is_multiple(price, rules.tick_size):
                    reasons.append(f"{field_name} {price} is not a multiple of the tick size {plain(rules.tick_size)}")
        try:
            quantity = size_order(amount=payload.quantity, quantity_type=payload.quantity_type,
                                  reference_price=inputs.reference_price, leverage=payload.leverage,
                                  rules=rules, entry_type=payload.entry_type)
        except SizingError as error:
            reasons.append(str(error))
            quantity = None
        reasons += self._price_guard(payload, inputs.mark_price)
        reasons += _protection_sides(payload, inputs.reference_price)
        active = self._positions.find_active(request.account_id, payload.symbol)
        if active is not None:
            reasons.append(f"Account {request.account_id} already has an active position on {payload.symbol} "
                           f"({active.position_id}).")
        if reasons:
            return self._reject(request, reasons)

        try:
            position = self._positions.create(NewPosition(
                position_id=self._generate_position_id(),
                account_id=request.account_id,
                symbol=payload.symbol,
                direction=payload.direction.value,
                quantity=quantity,
                requested_quantity=payload.quantity,
                quantity_type=payload.quantity_type.value,
                leverage=payload.leverage,
                entry_type=payload.entry_type.value,
                entry_price=payload.entry_price,
                take_profit_price=payload.take_profit_price,
                stop_loss_price=payload.stop_loss_price,
                open_request_id=request.request_id,
            ))
        except PositionConflictError as error:
            return self._reject(request, [str(error)])
        return self._accept(request, position)

    def _price_guard(self, payload: OpenPayload, mark_price: Decimal | None) -> list[str]:
        """MARKET entries only: refuse when Django's entry_price is too far from the mark price."""
        limit = self._settings.price_guard_max_deviation_percent
        if limit is None or payload.entry_type is not EntryType.MARKET or payload.entry_price is None:
            return []
        deviation = abs(mark_price - payload.entry_price) / payload.entry_price * 100
        if deviation > limit:
            return [f"price guard: the mark price {plain(mark_price)} is {plain(round(deviation, 2))}% from "
                    f"entry_price {plain(payload.entry_price)}, more than the allowed {plain(limit)}%"]
        return []

    # --- CLOSE --------------------------------------------------------------------------------

    def _decide_close(self, request: Request, payload: ClosePayload) -> IntakeOutcome:
        position = self._close_target(request.account_id, payload)
        if position is None:
            target = payload.position_id or payload.symbol
            reason = (f"Position {payload.position_id} was not found for account {request.account_id}."
                      if payload.position_id else
                      f"Account {request.account_id} has no active position on {target}.")
            return self._reject(request, [reason])
        if not is_active(position.state):
            # Already finished (e.g. its take-profit filled first): answer with the final result.
            accepted = self._requests.transition(request.request_id, RequestStatus.ACCEPTED,
                                                 position_id=position.position_id)
            self._requests.claim(accepted.request_id)
            processed = self._requests.mark_processed(accepted.request_id)
            body = receipt(processed, position, already_closed=True)
            self._requests.set_response(request.request_id, body)
            return IntakeOutcome(200, body)
        return self._accept(request, position)

    def _close_target(self, account_id: str, payload: ClosePayload) -> Position | None:
        if payload.position_id is not None:
            try:
                position = self._positions.get(payload.position_id)
            except PositionNotFoundError:
                return None
            return position if position.account_id == account_id else None
        return self._positions.find_active(account_id, payload.symbol)

    # --- outcomes -----------------------------------------------------------------------------

    def _accept(self, request: Request, position: Position) -> IntakeOutcome:
        accepted = self._requests.transition(request.request_id, RequestStatus.ACCEPTED,
                                             position_id=position.position_id)
        body = receipt(accepted, position)
        self._requests.set_response(request.request_id, body)
        return IntakeOutcome(202, body)

    def _reject(self, request: Request, reasons: list[str]) -> IntakeOutcome:
        rejected = self._requests.transition(request.request_id, RequestStatus.REJECTED, rejection_reasons=reasons)
        body = receipt(rejected, None)
        self._requests.set_response(request.request_id, body)
        return IntakeOutcome(422, body)

    def _duplicate(self, existing: Request, body_hash: str) -> IntakeOutcome:
        if existing.payload_hash != body_hash:
            return IntakeOutcome(409, {
                "error": "request_id_conflict",
                "message": f"request_id {existing.request_id} was already used for a different request.",
                "request_id": existing.request_id,
            })
        original = existing.response or receipt(existing, None)
        return IntakeOutcome(200, {**original, "duplicate": True})


def _parse_payload(action: RequestAction, fields: dict[str, Any]) -> tuple[OpenPayload | ClosePayload | None, list[str]]:
    model = OpenPayload if action is RequestAction.OPEN else ClosePayload
    try:
        return model.model_validate(fields), []
    except ValidationError as error:
        return None, [f"{detail['field']}: {detail['message']}" for detail in _details(error)]


def _protection_sides(payload: OpenPayload, reference_price: Decimal) -> list[str]:
    """A LONG's take-profit sits above the price and its stop-loss below; a SHORT's the reverse."""
    reasons = []
    is_long = payload.direction.value == "LONG"
    take_profit, stop_loss = payload.take_profit_price, payload.stop_loss_price
    if take_profit is not None and (take_profit <= reference_price if is_long else take_profit >= reference_price):
        side = "above" if is_long else "below"
        reasons.append(f"take_profit_price {take_profit} must be {side} the entry reference {plain(reference_price)} "
                       f"for a {payload.direction.value}")
    if stop_loss is not None and (stop_loss >= reference_price if is_long else stop_loss <= reference_price):
        side = "below" if is_long else "above"
        reasons.append(f"stop_loss_price {stop_loss} must be {side} the entry reference {plain(reference_price)} "
                       f"for a {payload.direction.value}")
    return reasons


def _details(error: ValidationError) -> list[dict[str, str]]:
    return [
        {"field": ".".join(str(part) for part in field_error["loc"]) or "request", "message": field_error["msg"]}
        for field_error in error.errors(include_url=False)
    ]
