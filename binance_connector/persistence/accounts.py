"""Accounts: one per Django account_id, owned by exactly one user (spec §4).

Credentials arrive with every request. ``upsert`` stores them encrypted the first time,
replaces them when they change (decision I: the latest request wins), and refuses an
account_id that already belongs to another user. That is how the service keeps users
apart even if Django ever sent a wrong pairing.

Snapshots never carry credentials. ``credentials()`` decrypts them for the Binance client
and registers both values with the log redactor, so they cannot leak into logs afterwards.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from binance_connector.errors import AccountNotFoundError, AccountOwnershipError
from binance_connector.persistence.models import AccountRecord
from binance_connector.persistence.writes import utc_now, write_with_retry
from binance_connector.security.credentials import ApiCredentials, CredentialCipher, fingerprint
from binance_connector.security.redaction import REDACTOR, SecretRedactor


@dataclass(frozen=True)
class Account:
    """An account as the rest of the service sees it: no credentials."""

    account_id: str
    user_id: str
    api_key_fingerprint: str
    credentials_updated_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AccountUpsert:
    account: Account
    created: bool
    credentials_changed: bool


class AccountRepository:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        cipher: CredentialCipher,
        redactor: SecretRedactor = REDACTOR,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._session_factory = session_factory
        self._cipher = cipher
        self._redactor = redactor
        self._clock = clock

    def upsert(self, account_id: str, user_id: str, credentials: ApiCredentials) -> AccountUpsert:
        """Create the account, or confirm its owner and refresh its credentials.

        Raises:
            AccountOwnershipError: the account already belongs to a different user.
        """
        self._redactor.register(credentials.api_key, credentials.api_secret)
        if self._find(account_id) is None:
            try:
                return self._create(account_id, user_id, credentials)
            except IntegrityError:
                pass  # created concurrently by another request: fall through to the update path

        outcome = {}

        def load(session: Session) -> AccountRecord:
            return self._load(session, account_id)

        def change(_session: Session, record: AccountRecord) -> None:
            if record.user_id != user_id:
                raise AccountOwnershipError(account_id)
            changed = (
                record.api_key_fingerprint != fingerprint(credentials.api_key)
                or self._cipher.decrypt(record.api_secret_encrypted) != credentials.api_secret
            )
            outcome["changed"] = changed
            if changed:
                now = self._clock()
                record.api_key_encrypted = self._cipher.encrypt(credentials.api_key)
                record.api_secret_encrypted = self._cipher.encrypt(credentials.api_secret)
                record.api_key_fingerprint = fingerprint(credentials.api_key)
                record.credentials_updated_at = now
                record.updated_at = now

        account = write_with_retry(self._session_factory, load, change, lambda _session, record: _snapshot(record))
        return AccountUpsert(account=account, created=False, credentials_changed=outcome["changed"])

    def get(self, account_id: str) -> Account:
        """Raises AccountNotFoundError."""
        record = self._find(account_id)
        if record is None:
            raise AccountNotFoundError(account_id)
        return _snapshot(record)

    def credentials(self, account_id: str) -> ApiCredentials:
        """Decrypt the account's credentials (and register them for log redaction)."""
        record = self._find(account_id)
        if record is None:
            raise AccountNotFoundError(account_id)
        credentials = ApiCredentials(
            api_key=self._cipher.decrypt(record.api_key_encrypted),
            api_secret=self._cipher.decrypt(record.api_secret_encrypted),
        )
        self._redactor.register(credentials.api_key, credentials.api_secret)
        return credentials

    def _create(self, account_id: str, user_id: str, credentials: ApiCredentials) -> AccountUpsert:
        now = self._clock()
        record = AccountRecord(
            account_id=account_id,
            user_id=user_id,
            api_key_encrypted=self._cipher.encrypt(credentials.api_key),
            api_secret_encrypted=self._cipher.encrypt(credentials.api_secret),
            api_key_fingerprint=fingerprint(credentials.api_key),
            created_at=now,
            updated_at=now,
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()
            return AccountUpsert(account=_snapshot(record), created=True, credentials_changed=False)

    def _find(self, account_id: str) -> AccountRecord | None:
        with self._session_factory() as session:
            return session.scalar(select(AccountRecord).where(AccountRecord.account_id == account_id))

    @staticmethod
    def _load(session: Session, account_id: str) -> AccountRecord:
        record = session.scalar(select(AccountRecord).where(AccountRecord.account_id == account_id))
        if record is None:
            raise AccountNotFoundError(account_id)
        return record


def _snapshot(record: AccountRecord) -> Account:
    return Account(
        account_id=record.account_id,
        user_id=record.user_id,
        api_key_fingerprint=record.api_key_fingerprint,
        credentials_updated_at=record.credentials_updated_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
