"""Unit tests for the per-account Binance client cache."""

from binance_connector.security.credentials import ApiCredentials
from binance_connector.services.account_clients import AccountClients


class RecordingFactory:
    def __init__(self) -> None:
        self.built: list[ApiCredentials] = []

    def __call__(self, settings, credentials):
        self.built.append(credentials)
        return ("client-for", credentials.api_key)


def test_each_account_gets_a_client_with_its_own_credentials(accounts, settings):
    factory = RecordingFactory()
    clients = AccountClients(accounts=accounts, settings=settings, client_factory=factory)

    first_client = clients.for_account("account-1")
    second_client = clients.for_account("account-2")

    assert first_client == ("client-for", "key-account-1")
    assert second_client == ("client-for", "key-account-2")


def test_a_client_is_reused_while_the_credentials_stay_the_same(accounts, settings):
    factory = RecordingFactory()
    clients = AccountClients(accounts=accounts, settings=settings, client_factory=factory)

    clients.for_account("account-1")
    clients.for_account("account-1")

    assert len(factory.built) == 1


def test_rotated_credentials_get_a_new_client(accounts, settings):
    factory = RecordingFactory()
    clients = AccountClients(accounts=accounts, settings=settings, client_factory=factory)
    clients.for_account("account-1")

    accounts.upsert("account-1", "user-of-account-1", ApiCredentials("rotated-key-1", "rotated-secret-1"))

    assert clients.for_account("account-1") == ("client-for", "rotated-key-1")
    assert len(factory.built) == 2
