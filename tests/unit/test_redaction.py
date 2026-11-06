"""Unit tests for scrubbing secrets out of log output."""

import logging
import sys

import pytest

from binance_connector.security.redaction import MASK, RedactingFilter, SecretRedactor


@pytest.fixture
def redactor() -> SecretRedactor:
    return SecretRedactor()


def test_registered_secrets_are_masked_in_text(redactor):
    redactor.register("super-secret-value", "api-key-value")

    assert redactor.redact("key=api-key-value secret=super-secret-value") == f"key={MASK} secret={MASK}"


def test_short_or_empty_values_are_not_registered(redactor):
    redactor.register("", "abc")

    assert redactor.redact("abc is fine") == "abc is fine"


def test_the_longer_secret_is_masked_first_when_one_contains_another(redactor):
    redactor.register("secret", "secret-and-more")

    assert redactor.redact("x secret-and-more y") == f"x {MASK} y"


def make_logger(redactor, caplog):
    logger = logging.getLogger("test.redaction")
    caplog.set_level(logging.INFO, logger="test.redaction")
    caplog.handler.addFilter(RedactingFilter(redactor))
    return logger


def test_log_messages_and_arguments_are_masked(redactor, caplog):
    redactor.register("super-secret-value")
    logger = make_logger(redactor, caplog)

    logger.info("calling with %s", "super-secret-value")

    assert "super-secret-value" not in caplog.text
    assert MASK in caplog.text


def test_two_redacting_filters_in_a_row_both_take_effect(redactor):
    """Found by the full suite: a first filter must not leave the traceback unredactable."""
    first_redactor, second_redactor = SecretRedactor(), redactor
    first_redactor.register("first-secret-value")
    second_redactor.register("second-secret-value")
    try:
        raise RuntimeError("first-secret-value and second-secret-value")
    except RuntimeError:
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", None, sys.exc_info())

    RedactingFilter(first_redactor).filter(record)
    RedactingFilter(second_redactor).filter(record)
    rendered = logging.Formatter().format(record)

    assert "first-secret-value" not in rendered
    assert "second-secret-value" not in rendered
    assert record.exc_info is not None  # other handlers keep working


def test_tracebacks_are_masked(redactor, caplog):
    redactor.register("super-secret-value")
    logger = make_logger(redactor, caplog)

    try:
        raise RuntimeError("failed for super-secret-value")
    except RuntimeError:
        logger.exception("request failed")

    assert "super-secret-value" not in caplog.text
    assert "RuntimeError" in caplog.text
