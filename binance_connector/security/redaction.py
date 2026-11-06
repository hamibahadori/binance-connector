"""Keep secrets out of log output (spec §10: never in logs or error reports).

Every credential the service decrypts is registered with the process-wide ``REDACTOR``.
``install_log_redaction`` adds a filter to the root logging handlers that replaces any
registered value, in messages, arguments and tracebacks, with ``***``.

Registration-based masking is exact: it catches a secret wherever it appears, including in
third-party log lines, without guessing at patterns.
"""

import logging
import threading

MASK = "***"
MIN_SECRET_LENGTH = 6
"""Shorter values are not registered: masking them would garble ordinary text."""


class SecretRedactor:
    """A thread-safe set of strings that must never appear in output."""

    def __init__(self) -> None:
        self._secrets: set[str] = set()
        self._lock = threading.Lock()

    def register(self, *values: str) -> None:
        with self._lock:
            self._secrets.update(value for value in values if value and len(value) >= MIN_SECRET_LENGTH)

    def redact(self, text: str) -> str:
        with self._lock:
            secrets_longest_first = sorted(self._secrets, key=len, reverse=True)
        for secret in secrets_longest_first:
            text = text.replace(secret, MASK)
        return text


class RedactingFilter(logging.Filter):
    """A logging filter that masks registered secrets. It never drops records."""

    def __init__(self, redactor: SecretRedactor) -> None:
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted_message = self._redactor.redact(message)
        if redacted_message != message:
            record.msg = redacted_message
            record.args = None
        # Redact the traceback text in place. If another handler or filter already rendered
        # it (``exc_text``), build on that rather than re-rendering from ``exc_info``: the
        # earlier redaction must survive. ``exc_info`` is kept, so other handlers still work,
        # and formatters reuse the cached, redacted ``exc_text``.
        if record.exc_info or record.exc_text:
            traceback_text = record.exc_text or logging.Formatter().formatException(record.exc_info)
            record.exc_text = self._redactor.redact(traceback_text)
        if record.stack_info:
            record.stack_info = self._redactor.redact(record.stack_info)
        return True


REDACTOR = SecretRedactor()


def install_log_redaction(redactor: SecretRedactor = REDACTOR) -> None:
    """Attach the redacting filter to every root handler (call after ``logging.basicConfig``)."""
    for handler in logging.getLogger().handlers:
        if not any(isinstance(existing, RedactingFilter) for existing in handler.filters):
            handler.addFilter(RedactingFilter(redactor))
