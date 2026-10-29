"""Saved account lookup and endpoint resolution."""

from emailsync.errors import AuthError
from emailsync.options import Endpoint

ACCOUNT_CONNECTION_FIELDS = ("url", "host", "port", "security", "username", "insecure_tls")


def load_accounts(store, secretbox):
    accounts = {}
    for account in store.list_accounts():
        account["secrets"] = secretbox.decrypt(account.get("secrets_enc"))
        accounts[int(account["id"])] = account
    return accounts


def resolve_endpoint(endpoint: Endpoint | None, accounts, token_manager=None) -> Endpoint | None:
    if endpoint is None or endpoint.account_id is None:
        return endpoint
    account = accounts.get(int(endpoint.account_id))
    if account is None:
        raise AuthError(f"Saved account {endpoint.account_id} no longer exists.")
    config = account.get("config", {})
    secrets = account.get("secrets", {})
    for field in ACCOUNT_CONNECTION_FIELDS:
        if field in config:
            setattr(endpoint, field, config[field])
    auth_method = config.get("auth_method", "password")
    endpoint.password = secrets.get("password", "") if auth_method == "password" else ""
    endpoint.token = secrets.get("token", "") if auth_method == "token" else ""
    endpoint.token_provider = None
    if auth_method == "oauth":
        if token_manager is None:
            raise AuthError(f"OAuth sign-in for account '{account['name']}' is unavailable.")
        endpoint.token_provider = token_manager.provider(int(endpoint.account_id))
    return endpoint
