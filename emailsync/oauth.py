"""OAuth authorization-code flows and refreshable bearer tokens."""

import base64
import hashlib
import inspect
import json
import os
import secrets
import threading
import time
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import httpx

from emailsync.errors import AuthError, PermanentError, ReauthRequired, TransientError

DEFAULT_SCOPES = ("openid", "offline_access", "email", "profile")
MIGRATION_SCOPES = (
    "openid",
    "offline_access",
    "urn:ietf:params:oauth:scope:mail",
    "urn:ietf:params:oauth:scope:contacts",
    "urn:ietf:params:oauth:scope:calendars",
)
PENDING_TTL = 15 * 60


def _rebase(url, base):
    source, target = urlsplit(url), urlsplit(base)
    return urlunsplit((target.scheme, target.netloc, source.path, source.query, source.fragment))


def _request_error(response, context):
    if response.status_code >= 500:
        raise TransientError(f"{context} returned HTTP {response.status_code}")
    if response.status_code in (401, 403):
        raise AuthError(f"{context} returned HTTP {response.status_code}")
    if response.status_code >= 400:
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error") or response.text
        except ValueError:
            detail = response.text
        raise PermanentError(f"{context}: {detail}")


def _metadata_url(url, oidc=False):
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/")
    if oidc:
        return url.rstrip("/") + "/.well-known/openid-configuration"
    return urlunsplit(
        (parsed.scheme, parsed.netloc, "/.well-known/oauth-authorization-server" + path, "", "")
    )


def discover(url_or_issuer, insecure_tls=False, rebase_to=None, transport=None):
    """Discover OAuth metadata, preserving its advertised issuer for validation."""
    issuer_url = url_or_issuer.rstrip("/")
    try:
        with httpx.Client(
            verify=not insecure_tls, follow_redirects=True, timeout=30, transport=transport
        ) as client:
            response = client.get(_metadata_url(issuer_url))
            if response.status_code == 404:
                response = client.get(_metadata_url(issuer_url, oidc=True))
            _request_error(response, "OAuth metadata discovery")
            metadata = response.json()
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        raise TransientError(f"OAuth metadata discovery failed: {exc}") from exc
    except ValueError as exc:
        raise PermanentError("OAuth metadata endpoint returned invalid JSON") from exc
    if not isinstance(metadata, dict) or not metadata.get("issuer"):
        raise PermanentError("OAuth metadata is missing an issuer")
    if rebase_to:
        configured_host = (urlsplit(rebase_to).hostname or "").casefold()
        advertised_host = (urlsplit(metadata["issuer"]).hostname or "").casefold()
        if configured_host and advertised_host and configured_host != advertised_host:
            for key in (
                "authorization_endpoint",
                "token_endpoint",
                "registration_endpoint",
                "userinfo_endpoint",
                "device_authorization_endpoint",
            ):
                if metadata.get(key):
                    metadata[key] = _rebase(metadata[key], rebase_to)
    return metadata


def register_client(metadata, redirect_uri, insecure_tls=False, transport=None, scopes=None):
    endpoint = metadata.get("registration_endpoint")
    if not endpoint:
        raise PermanentError("This server doesn't support automatic registration; enter a client ID")
    payload = {
        "client_name": "email-sync",
        "redirect_uris": [redirect_uri],
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
    }
    if scopes:
        payload["scope"] = " ".join(scopes)
    try:
        with httpx.Client(
            verify=not insecure_tls, follow_redirects=True, timeout=30, transport=transport
        ) as client:
            response = client.post(endpoint, json=payload)
            _request_error(response, "OAuth client registration")
            result = response.json()
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        raise TransientError(f"OAuth client registration failed: {exc}") from exc
    except ValueError as exc:
        raise PermanentError("OAuth registration endpoint returned invalid JSON") from exc
    if not result.get("client_id"):
        raise PermanentError("OAuth registration response did not include a client ID")
    return result


def default_scopes(metadata):
    supported = metadata.get("scopes_supported")
    if isinstance(supported, list):
        return [scope for scope in MIGRATION_SCOPES if scope in supported]
    return list(DEFAULT_SCOPES)


def _oauth_post(url, data, config, client_secret, *, insecure_tls=False, refresh=False, transport=None):
    methods = config.get("token_endpoint_auth_methods_supported") or []
    auth = None
    if client_secret and "client_secret_basic" in methods:
        auth = (config["client_id"], client_secret)
        data = {key: value for key, value in data.items() if key not in ("client_id", "client_secret")}
    elif client_secret and "client_secret_post" in methods:
        data = {**data, "client_secret": client_secret}
    try:
        with httpx.Client(
            verify=not insecure_tls, follow_redirects=True, timeout=30, transport=transport
        ) as client:
            response = client.post(url, data=data, auth=auth)
            if refresh and response.status_code == 400:
                try:
                    if response.json().get("error") == "invalid_grant":
                        return {"error": "invalid_grant"}
                except ValueError:
                    pass
            _request_error(response, "OAuth token endpoint")
            return response.json()
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        raise TransientError(f"OAuth token request failed: {exc}") from exc
    except ValueError as exc:
        raise PermanentError("OAuth token endpoint returned invalid JSON") from exc


def _query_params(value):
    if "://" in value:
        query = urlsplit(value).query
    else:
        query = value.lstrip("?")
    return {key: values[-1] for key, values in parse_qs(query, keep_blank_values=True).items()}


def _id_token_email(value):
    try:
        payload = value.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return claims.get("email", "")
    except (IndexError, ValueError, TypeError):
        return ""


class OAuthService:
    def __init__(self, store, secretbox, redirect_uri=None, transport=None):
        self.store = store
        self.secretbox = secretbox
        self.transport = transport
        self.redirect_uri = redirect_uri or os.environ.get(
            "EMAIL_SYNC_OAUTH_REDIRECT_URI", "http://127.0.0.1:8080/oauth/callback"
        )
        self.pending = {}
        self.lock = threading.Lock()

    def _account(self, account_id):
        account = self.store.get_account(account_id)
        if not account:
            raise PermanentError(f"Saved account {account_id} was not found.")
        account["secrets"] = self.secretbox.decrypt(account.get("secrets_enc"))
        return account

    def _save(self, account):
        self.store.update_account_config(account["id"], account["name"], account["config"])
        self.store.update_account_secrets(
            account["id"],
            self.secretbox.encrypt(account["secrets"]) if account["secrets"] else None,
        )

    def start(self, account):
        if isinstance(account, dict):
            account_id = account.get("id")
        else:
            account_id = getattr(account, "id", account)
        if account_id is None:
            raise PermanentError("Saved account was not found.")
        account = self._account(account_id)
        config, secret = account["config"], account["secrets"]
        if config.get("auth_method") != "oauth":
            raise PermanentError("Select OAuth as the account authentication method first.")
        issuer_url = config.get("issuer_url") or config.get("url") or ""
        metadata = discover(
            issuer_url,
            bool(config.get("insecure_tls")),
            rebase_to=None if config.get("issuer_url") else config.get("url"),
            transport=self.transport,
        )
        scopes = str(config.get("scopes") or "").split()
        changed = False
        if not scopes:
            scopes = default_scopes(metadata)
            config["scopes"] = " ".join(scopes)
            changed = True
        if not config.get("client_id"):
            registration = register_client(
                metadata,
                self.redirect_uri,
                bool(config.get("insecure_tls")),
                transport=self.transport,
                scopes=scopes,
            )
            config["client_id"] = registration["client_id"]
            if registration.get("client_secret"):
                secret["client_secret"] = registration["client_secret"]
            changed = True
        if changed:
            self._save(account)
        verifier = secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        state = secrets.token_urlsafe(32)
        expires = time.time() + PENDING_TTL
        with self.lock:
            self.pending = {key: flow for key, flow in self.pending.items() if flow["expires"] > time.time()}
            self.pending[state] = {
                "account_id": account_id,
                "metadata": metadata,
                "verifier": verifier,
                "expires": expires,
                "scopes": scopes,
                "redirect_uri": self.redirect_uri,
            }
        query = urlencode(
            {
                "response_type": "code",
                "client_id": config["client_id"],
                "redirect_uri": self.redirect_uri,
                "scope": " ".join(scopes),
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
        return (
            metadata["authorization_endpoint"]
            + ("&" if "?" in metadata["authorization_endpoint"] else "?")
            + query
        )

    def complete(self, callback_url_or_query):
        params = _query_params(callback_url_or_query)
        state = params.get("state", "")
        with self.lock:
            flow = self.pending.pop(state, None)
        if not flow:
            raise PermanentError("OAuth sign-in state is unknown, expired, or already used.")
        if flow["expires"] < time.time():
            raise PermanentError("OAuth sign-in state has expired. Start sign-in again.")
        if params.get("error"):
            detail = params.get("error_description") or params["error"]
            raise PermanentError(f"OAuth sign-in failed: {detail}")
        if not params.get("code"):
            raise PermanentError("OAuth callback did not include an authorization code.")
        account = self._account(flow["account_id"])
        config, secret, metadata = account["config"], account["secrets"], flow["metadata"]
        if metadata.get("authorization_response_iss_parameter_supported") and params.get("iss"):
            if params["iss"] != metadata["issuer"]:
                raise PermanentError("OAuth callback issuer does not match the discovered issuer.")
        result = _oauth_post(
            metadata["token_endpoint"],
            {
                "grant_type": "authorization_code",
                "code": params["code"],
                "redirect_uri": flow["redirect_uri"],
                "code_verifier": flow["verifier"],
                "client_id": config["client_id"],
            },
            metadata,
            secret.get("client_secret", ""),
            insecure_tls=bool(config.get("insecure_tls")),
            transport=self.transport,
        )
        if result.get("error"):
            raise PermanentError(f"OAuth token exchange failed: {result['error']}")
        if not result.get("access_token"):
            raise PermanentError("OAuth token response did not include an access token.")
        secret["access_token"] = result["access_token"]
        if result.get("refresh_token"):
            secret["refresh_token"] = result["refresh_token"]
        secret["expires_at"] = time.time() + int(result.get("expires_in", 3600))
        if not config.get("username"):
            config["username"] = _id_token_email(result.get("id_token", ""))
        self._save(account)
        return account


class TokenManager:
    def __init__(self, store, secretbox, transport=None):
        self.store = store
        self.secretbox = secretbox
        self.transport = transport
        self._providers = {}
        self._locks = {}
        self._lock = threading.Lock()

    def provider(self, account_id):
        with self._lock:
            if account_id not in self._providers:
                self._providers[account_id] = _TokenProvider(self, account_id)
                self._locks[account_id] = threading.Lock()
            return self._providers[account_id]

    def _token(self, account_id, force_refresh=False, rejected_token=None):
        previous_access = None
        if force_refresh and rejected_token is None:
            current = self.store.get_account(account_id)
            if current:
                previous_access = self.secretbox.decrypt(current.get("secrets_enc")).get("access_token")
        with self._lock:
            lock = self._locks.setdefault(account_id, threading.Lock())
        with lock:
            account = self.store.get_account(account_id)
            if not account:
                raise ReauthRequired(f"account {account_id}")
            config = account["config"]
            secret = self.secretbox.decrypt(account.get("secrets_enc"))
            rejected = rejected_token or previous_access
            if force_refresh and secret.get("access_token") and secret.get("access_token") != rejected:
                return secret["access_token"]
            if (
                not force_refresh
                and secret.get("access_token")
                and float(secret.get("expires_at", 0)) >= time.time() + 60
            ):
                return secret["access_token"]
            refresh_token = secret.get("refresh_token")
            if not refresh_token:
                raise ReauthRequired(account["name"])
            issuer_url = config.get("issuer_url") or config.get("url") or ""
            metadata = discover(
                issuer_url,
                bool(config.get("insecure_tls")),
                rebase_to=None if config.get("issuer_url") else config.get("url"),
                transport=self.transport,
            )
            result = _oauth_post(
                metadata["token_endpoint"],
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": config.get("client_id", ""),
                },
                metadata,
                secret.get("client_secret", ""),
                insecure_tls=bool(config.get("insecure_tls")),
                refresh=True,
                transport=self.transport,
            )
            if result.get("error") == "invalid_grant":
                raise ReauthRequired(account["name"])
            if not result.get("access_token"):
                raise PermanentError("OAuth refresh response did not include an access token.")
            secret["access_token"] = result["access_token"]
            secret["refresh_token"] = result.get("refresh_token") or refresh_token
            secret["expires_at"] = time.time() + int(result.get("expires_in", 3600))
            self.store.update_account_secrets(account_id, self.secretbox.encrypt(secret))
            return secret["access_token"]


class _TokenProvider:
    def __init__(self, manager, account_id):
        self.manager = manager
        self.account_id = account_id

    def token(self, force_refresh=False, rejected_token=None):
        return self.manager._token(self.account_id, force_refresh, rejected_token)


class OAuthBearerAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, provider):
        self.provider = provider

    def auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.provider.token()}"
        response = yield request
        if response.status_code == 401:
            response.read()
            rejected = request.headers.get("Authorization", "").removeprefix("Bearer ")
            request.headers["Authorization"] = f"Bearer {refresh_after_rejection(self.provider, rejected)}"
            yield request


class StaticBearerAuth(httpx.Auth):
    def __init__(self, token):
        self.value = f"Bearer {token}"

    def auth_flow(self, request):
        request.headers["Authorization"] = self.value
        yield request


def refresh_after_rejection(provider, rejected_token):
    method = provider.token
    if "rejected_token" in inspect.signature(method).parameters:
        return method(force_refresh=True, rejected_token=rejected_token)
    return method(force_refresh=True)
