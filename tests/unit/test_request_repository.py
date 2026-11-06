"""Unit tests for stored requests: lifecycle, duplicates, and the worker's queue."""

from datetime import UTC, datetime

import pytest

from binance_connector.domain.lifecycle import ProcessingState, RequestAction, RequestStatus
from binance_connector.errors import DuplicateRequestError, InvalidTransitionError, RequestNotFoundError
from binance_connector.persistence.accounts import AccountRepository
from binance_connector.persistence.requests import NewRequest, RequestRepository
from binance_connector.security.credentials import ApiCredentials
from binance_connector.security.redaction import SecretRedactor

REQUESTED_AT = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


@pytest.fixture
def repository(session_factory, cipher) -> RequestRepository:
    accounts = AccountRepository(session_factory, cipher=cipher, redactor=SecretRedactor())
    accounts.upsert("account-1", "user-1", ApiCredentials("demo-api-key-0001", "demo-api-secret-0001"))
    return RequestRepository(session_factory)


def new_request(request_id="request-1", action=RequestAction.OPEN, payload=None) -> NewRequest:
    return NewRequest(
        request_id=request_id,
        account_id="account-1",
        user_id="user-1",
        action=action,
        payload=payload or {"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"},
        payload_hash="hash-" + request_id,
        requested_at=REQUESTED_AT,
    )


def test_a_new_request_starts_received_with_its_payload(repository):
    request = repository.create(new_request())

    assert request.status is RequestStatus.RECEIVED
    assert request.action is RequestAction.OPEN
    assert request.payload == {"symbol": "BTCUSDT", "direction": "LONG", "quantity": "0.002"}
    assert request.requested_at == REQUESTED_AT
    assert request.received_at.tzinfo is UTC
    assert request.processing_state is None


def test_a_repeated_request_id_raises_duplicate_with_the_original(repository):
    repository.create(new_request())

    with pytest.raises(DuplicateRequestError) as raised:
        repository.create(new_request(payload={"different": "body"}))

    assert raised.value.existing.payload_hash == "hash-request-1"


def test_accepting_puts_the_request_in_the_queue(repository):
    repository.create(new_request())
    repository.transition("request-1", RequestStatus.VALIDATING)

    request = repository.transition("request-1", RequestStatus.ACCEPTED, position_id="p" * 24,
                                    response={"status": "ACCEPTED"})

    assert request.processing_state is ProcessingState.PENDING
    assert request.decided_at is not None
    assert request.position_id == "p" * 24
    assert request.response == {"status": "ACCEPTED"}
    assert [pending.request_id for pending in repository.pending()] == ["request-1"]


def test_rejected_requests_never_enter_the_queue(repository):
    repository.create(new_request())

    request = repository.transition("request-1", RequestStatus.REJECTED,
                                    rejection_reasons=["quantity below the minimum"])

    assert request.rejection_reasons == ["quantity below the minimum"]
    assert request.processing_state is None
    assert repository.pending() == []


def test_impossible_status_changes_are_refused(repository):
    repository.create(new_request())

    with pytest.raises(InvalidTransitionError):
        repository.transition("request-1", RequestStatus.ACCEPTED)


def accept(repository, request_id):
    repository.create(new_request(request_id))
    repository.transition(request_id, RequestStatus.VALIDATING)
    repository.transition(request_id, RequestStatus.ACCEPTED)


def test_only_one_worker_can_claim_a_request(repository):
    accept(repository, "request-1")

    assert repository.claim("request-1") is True
    assert repository.claim("request-1") is False
    assert repository.pending() == []


def test_processed_requests_leave_the_queue_for_good(repository):
    accept(repository, "request-1")
    repository.claim("request-1")

    request = repository.mark_processed("request-1")

    assert request.processing_state is ProcessingState.DONE
    assert request.processed_at is not None


def test_claims_left_by_a_crash_are_released_on_start_up(repository):
    accept(repository, "request-1")
    accept(repository, "request-2")
    repository.claim("request-1")

    released_count = repository.release_in_progress()

    assert released_count == 1
    assert [pending.request_id for pending in repository.pending()] == ["request-1", "request-2"]


def test_the_queue_is_in_arrival_order(repository):
    for request_id in ("request-b", "request-a", "request-c"):
        accept(repository, request_id)

    assert [pending.request_id for pending in repository.pending()] == ["request-b", "request-a", "request-c"]


def test_unknown_requests_raise_not_found(repository):
    with pytest.raises(RequestNotFoundError):
        repository.get("unknown")
    assert repository.find("unknown") is None
