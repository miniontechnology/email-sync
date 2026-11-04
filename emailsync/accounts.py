"""Saved account lookup and endpoint resolution."""

from emailsync.errors import AuthError, PermanentError
from emailsync.options import Endpoint
from emailsync.providers import PROVIDER_PRESETS

ACCOUNT_CONNECTION_FIELDS = ("url", "host", "port", "security", "username", "insecure_tls")


def load_accounts(store, secretbox):
    accounts = {}
    for account in store.list_accounts():
        account["secrets"] = secretbox.decrypt(account.get("secrets_enc"))
        accounts[int(account["id"])] = account
    return accounts


def check_account_compat(endpoint: Endpoint, account) -> str | None:
    config = account.get("config", {})
    name = account.get("name", "Unknown")
    auth_method = config.get("auth_method", "password")
    preset = PROVIDER_PRESETS.get(config.get("type"))
    if preset:
        if endpoint.type not in preset["endpoints"]:
            return f"Account '{name}' is a Google account and can't be used for {endpoint.type.upper()}"
        if auth_method != "oauth":
            return f"Account '{name}' is a Google account and requires OAuth authentication"
        return None
    if endpoint.type in ("imap", "gmail") and auth_method == "token":
        return f"Account '{name}' uses a static token, which IMAP can't use; use a password or OAuth"
    if endpoint.type in ("imap", "gmail") and not config.get("host"):
        return f"Account '{name}' is missing a host required by IMAP"
    if endpoint.type in ("jmap", "carddav", "caldav") and not config.get("url"):
        return f"Account '{name}' is missing a URL required by {endpoint.type.upper()}"
    return None


def resolve_endpoint(endpoint: Endpoint | None, accounts, token_manager=None) -> Endpoint | None:
    if endpoint is None or endpoint.account_id is None:
        return endpoint
    account = accounts.get(int(endpoint.account_id))
    if account is None:
        raise AuthError(f"Saved account {endpoint.account_id} no longer exists.")
    if error := check_account_compat(endpoint, account):
        raise PermanentError(error)
    config = account.get("config", {})
    secrets = account.get("secrets", {})
    for field in ACCOUNT_CONNECTION_FIELDS:
        if field in config:
            setattr(endpoint, field, config[field])
    preset = PROVIDER_PRESETS.get(config.get("type"))
    if preset:
        for field, value in preset["endpoints"][endpoint.type].items():
            setattr(endpoint, field, value)
        endpoint.insecure_tls = False
    auth_method = config.get("auth_method", "password")
    endpoint.password = secrets.get("password", "") if auth_method == "password" else ""
    endpoint.token = secrets.get("token", "") if auth_method == "token" else ""
    endpoint.token_provider = None
    if auth_method == "oauth":
        if token_manager is None:
            raise AuthError(f"OAuth sign-in for account '{account['name']}' is unavailable.")
        endpoint.token_provider = token_manager.provider(int(endpoint.account_id))
    return endpoint
