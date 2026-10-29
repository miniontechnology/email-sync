import base64
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient

from emailsync.accounts import resolve_endpoint
from emailsync.config import Settings
from emailsync.connectors.dav import DavClient
from emailsync.crypto import SecretBox
from emailsync.db import Store
from emailsync.errors import AuthError, PermanentError, ReauthRequired
from emailsync.jobs.control import RunControl, TaskResult
from emailsync.jobs.manager import JobManager
from emailsync.oauth import (
    OAuthBearerAuth,
    OAuthService,
    TokenManager,
    _oauth_post,
    discover,
    register_client,
)
from emailsync.options import Endpoint, JobConfig, Options
from emailsync.web.app import _reauth_account_id, create_app


def _store(tmp_path):
    settings = Settings(tmp_path, "test-key", 2, True, "INFO")
    store = Store(settings.db_path)
    secretbox = SecretBox.from_settings(settings.secret_key, settings.data_dir)
    return settings, store, secretbox


def _account(store, secretbox, *, config=None, secrets=None, name="Saved"):
    config = config or {
        "type": "jmap",
        "url": "https://idp.example.test",
        "auth_method": "oauth",
        "username": "alice@example.test",
        "client_id": "client-1",
    }
    encrypted = secretbox.encrypt(secrets or {}) if secrets else None
    return store.create_account(name, config, encrypted)


def _metadata(issuer="https://idp.example.test"):
    return {
        "issuer": issuer,
        "authorization_endpoint": issuer + "/authorize",
        "token_endpoint": issuer + "/token",
        "registration_endpoint": issuer + "/register",
        "token_endpoint_auth_methods_supported": ["none"],
        "authorization_response_iss_parameter_supported": True,
        "scopes_supported": [
            "openid",
            "offline_access",
            "urn:ietf:params:oauth:scope:mail",
            "urn:ietf:params:oauth:scope:contacts",
        ],
    }


def test_discovery_handles_path_issuer_and_rebases_without_changing_issuer():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if request.url.path == "/.well-known/oauth-authorization-server/realms/demo":
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={
                **_metadata("https://inside.test/realms/demo"),
                "authorization_endpoint": "https://inside.test/login",
                "token_endpoint": "https://inside.test/token",
            },
        )

    meta = discover(
        "https://sso.test/realms/demo",
        rebase_to="https://sso.test/realms/demo",
        transport=httpx.MockTransport(handler),
    )
    assert seen == [
        "https://sso.test/.well-known/oauth-authorization-server/realms/demo",
        "https://sso.test/realms/demo/.well-known/openid-configuration",
    ]
    assert meta["issuer"] == "https://inside.test/realms/demo"
    assert meta["authorization_endpoint"] == "https://sso.test/login"
    assert meta["token_endpoint"] == "https://sso.test/token"


def test_explicit_issuer_discovery_does_not_rebase():
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=_metadata()))
    result = discover("https://idp.example.test/tenant", transport=transport)
    assert result["token_endpoint"] == "https://idp.example.test/token"


def test_oauth_registration_pkce_exchange_and_secrets_persist(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _account(store, secretbox, config={**_account_config(), "client_id": ""})
    seen = {}

    def handler(request):
        if request.url.path.endswith("oauth-authorization-server"):
            return httpx.Response(200, json=_metadata())
        if request.url.path == "/register":
            seen["registration"] = json.loads(request.content)
            return httpx.Response(201, json={"client_id": "registered-client"})
        if request.url.path == "/token":
            fields = dict(parse_qsl(request.content.decode()))
            seen["token_request"] = fields
            return httpx.Response(
                200,
                json={"access_token": "access-value", "refresh_token": "refresh-value", "expires_in": 3600},
            )
        return httpx.Response(404)

    service = OAuthService(
        store,
        secretbox,
        redirect_uri="http://127.0.0.1:8080/oauth/callback",
        transport=httpx.MockTransport(handler),
    )
    authorization_url = service.start(store.get_account(account_id))
    query = parse_qs(urlsplit(authorization_url).query)
    assert query["code_challenge_method"] == ["S256"]
    assert query["scope"] == [
        "openid offline_access urn:ietf:params:oauth:scope:mail urn:ietf:params:oauth:scope:contacts"
    ]
    assert seen["registration"] == {
        "client_name": "email-sync",
        "redirect_uris": ["http://127.0.0.1:8080/oauth/callback"],
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "scope": "openid offline_access urn:ietf:params:oauth:scope:mail urn:ietf:params:oauth:scope:contacts",
    }
    service.complete(
        "http://127.0.0.1:8080/oauth/callback?"
        + urlencode({"code": "auth-code", "state": query["state"][0], "iss": "https://idp.example.test"})
    )
    fields = seen["token_request"]
    expected = base64.urlsafe_b64encode(hashlib.sha256(fields["code_verifier"].encode()).digest())
    assert expected.decode().rstrip("=") == query["code_challenge"][0]
    account = store.get_account(account_id)
    secrets = secretbox.decrypt(account["secrets_enc"])
    assert account["config"]["client_id"] == "registered-client"
    assert secrets["access_token"] == "access-value"
    assert secrets["refresh_token"] == "refresh-value"
    assert "access-value" not in str(account)


def _account_config():
    return {
        "type": "jmap",
        "url": "https://idp.example.test",
        "auth_method": "oauth",
        "username": "alice@example.test",
    }


def test_oauth_state_error_expiry_reuse_and_issuer_validation(tmp_path, monkeypatch):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _account(store, secretbox)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json=_metadata() if request.url.path.endswith("oauth-authorization-server") else {},
        )
    )
    service = OAuthService(store, secretbox, transport=transport)
    url = service.start(account_id)
    query = parse_qs(urlsplit(url).query)
    with pytest.raises(Exception, match="state is unknown"):
        service.complete("?code=bad&state=other")
    with pytest.raises(Exception, match="issuer does not match"):
        service.complete("?code=code&state=" + query["state"][0] + "&iss=https%3A%2F%2Fevil.test")

    url = service.start(account_id)
    query = parse_qs(urlsplit(url).query)
    original_time = time.time
    monkeypatch.setattr("emailsync.oauth.time.time", lambda: original_time() + 16 * 60)
    with pytest.raises(Exception, match="state has expired"):
        service.complete("?code=code&state=" + query["state"][0])
    monkeypatch.setattr("emailsync.oauth.time.time", original_time)

    url = service.start(account_id)
    query = parse_qs(urlsplit(url).query)
    with pytest.raises(Exception, match="OAuth sign-in failed: No consent"):
        service.complete("?error=access_denied&error_description=No+consent&state=" + query["state"][0])
    with pytest.raises(Exception, match="state is unknown"):
        service.complete("?code=code&state=" + query["state"][0])


def test_token_refresh_rotates_refresh_token_and_serializes_five_threads(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _account(
        store,
        secretbox,
        secrets={
            "access_token": "old-access",
            "refresh_token": "old-refresh",
            "expires_at": time.time() + 30,
        },
    )
    calls = []

    def handler(request):
        if request.url.path.endswith("oauth-authorization-server"):
            return httpx.Response(200, json=_metadata())
        calls.append(dict(parse_qsl(request.content.decode())))
        time.sleep(0.03)
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "rotated-refresh",
                "expires_in": 3600,
            },
        )

    manager = TokenManager(store, secretbox, transport=httpx.MockTransport(handler))
    provider = manager.provider(account_id)
    with ThreadPoolExecutor(max_workers=5) as pool:
        tokens = list(pool.map(lambda _index: provider.token(), range(5)))
    assert tokens == ["new-access"] * 5
    assert len(calls) == 1
    assert calls[0]["refresh_token"] == "old-refresh"
    secrets = secretbox.decrypt(store.get_account(account_id)["secrets_enc"])
    assert secrets["refresh_token"] == "rotated-refresh"


def test_invalid_grant_requires_resignin_with_user_message(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _account(
        store,
        secretbox,
        secrets={"access_token": "old", "refresh_token": "revoked", "expires_at": 0},
        name="Work",
    )

    def handler(request):
        if request.url.path.endswith("oauth-authorization-server"):
            return httpx.Response(200, json=_metadata())
        return httpx.Response(400, json={"error": "invalid_grant"})

    provider = TokenManager(store, secretbox, transport=httpx.MockTransport(handler)).provider(account_id)
    with pytest.raises(ReauthRequired) as exc:
        provider.token()
    assert str(exc.value) == (
        "Sign-in for account 'Work' has expired or was revoked. "
        "Sign in again on the Accounts page, then resume the job."
    )


def test_http_bearer_auth_refreshes_once_after_401():
    class Provider:
        def __init__(self):
            self.value = "old"
            self.forced = 0

        def token(self, force_refresh=False, rejected_token=None):
            if force_refresh:
                self.forced += 1
                assert rejected_token == "old"
                self.value = "new"
            return self.value

    provider = Provider()
    seen = []

    def handler(request):
        seen.append(request.headers["Authorization"])
        return httpx.Response(401 if len(seen) == 1 else 200)

    with httpx.Client(transport=httpx.MockTransport(handler), auth=OAuthBearerAuth(provider)) as client:
        assert client.get("https://mail.example.test/").status_code == 200
    assert seen == ["Bearer old", "Bearer new"]
    assert provider.forced == 1


def test_dav_bearer_is_not_sent_to_cross_origin_redirect(tmp_path):
    class Provider:
        def token(self, force_refresh=False, rejected_token=None):
            return "secret-bearer"

    client = DavClient(Endpoint("carddav", url="https://dav.example.test"))
    seen = []

    def handler(request):
        seen.append((str(request.url), request.headers.get("Authorization")))
        if request.url.host == "dav.example.test":
            return httpx.Response(302, headers={"Location": "https://other.example.test/target"})
        return httpx.Response(200, text="ok")

    client.http.close()
    client.auth = OAuthBearerAuth(Provider())
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    try:
        assert client._request("GET", client.base).text == "ok"
    finally:
        client.close()
    assert seen[0][1] == "Bearer secret-bearer"
    assert seen[1][1] is None


def test_endpoint_resolution_account_wins_and_attaches_provider():
    class Tokens:
        def provider(self, account_id):
            return ("provider", account_id)

    endpoint = Endpoint("jmap", account_id=7, url="https://manual", username="manual-user", password="manual")
    accounts = {
        7: {
            "name": "Saved",
            "config": {
                "url": "https://saved",
                "host": "imap.saved",
                "port": 993,
                "security": "ssl",
                "username": "account-user",
                "insecure_tls": True,
                "auth_method": "oauth",
            },
            "secrets": {"password": "saved-password", "access_token": "token"},
        }
    }
    resolved = resolve_endpoint(endpoint, accounts, Tokens())
    assert resolved.url == "https://saved"
    assert resolved.username == "account-user"
    assert resolved.password == ""
    assert resolved.token_provider == ("provider", 7)


def test_saved_endpoints_validate_without_manual_connection_fields_and_provider_is_runtime_only():
    source = Endpoint("imap", account_id=1)
    dest = Endpoint("jmap", account_id=1)
    dest.token_provider = object()
    config = JobConfig(
        "Saved",
        {
            "mail": {"source": source, "dest": dest},
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(),
    )
    public, _secrets = config.split()
    assert config.validate() == []
    assert "token_provider" not in public["endpoints"]["mail"]["dest"]


def test_account_secrets_redacted_and_delete_refused_when_job_uses_account(tmp_path):
    settings, store, secretbox = _store(tmp_path)
    app = create_app(settings, store)
    with TestClient(app) as client:
        response = client.post(
            "/accounts",
            data={
                "name": "OAuth account",
                "type": "jmap",
                "url": "https://idp.example.test",
                "auth_method": "oauth",
                "client_secret": "client-secret-value",
                "username": "alice@example.test",
            },
            follow_redirects=False,
        )
        account_id = int(response.headers["location"].split("/")[-1])
        for path in ("/accounts", f"/accounts/{account_id}", "/jobs/new", "/api/jobs"):
            assert "client-secret-value" not in client.get(path).text
        account = store.get_account(account_id)
        assert "client-secret-value" not in account["secrets_enc"]
        assert secretbox.decrypt(account["secrets_enc"])["client_secret"] == "client-secret-value"
        store.create_job(
            "Using account",
            {
                "endpoints": {
                    "mail": {
                        "source": {"type": "imap", "account_id": account_id},
                        "dest": {"type": "jmap", "account_id": account_id},
                    }
                }
            },
            None,
        )
        response = client.post(f"/accounts/{account_id}/delete", follow_redirects=True)
        assert "Using account" in response.text
        assert store.get_account(account_id) is not None


def test_reauth_job_links_to_account_named_in_error(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    first = _account(store, secretbox, name="First")
    second = _account(store, secretbox, name="Second")
    job = {
        "last_error": "Sign-in for account 'Second' has expired or was revoked. Sign in again.",
        "config": {
            "endpoints": {
                "mail": {
                    "source": {"account_id": first},
                    "dest": {"account_id": second},
                }
            }
        },
    }
    assert _reauth_account_id(job, store) == second


def test_automatic_registration_missing_endpoint_has_exact_actionable_error():
    with pytest.raises(
        PermanentError,
        match="This server doesn't support automatic registration; enter a client ID",
    ):
        register_client({}, "http://127.0.0.1:8080/oauth/callback")


@pytest.mark.parametrize(
    ("method", "expected_auth", "send_secret"),
    [("client_secret_basic", True, False), ("client_secret_post", False, True), ("none", False, False)],
)
def test_token_exchange_uses_supported_client_auth_method(method, expected_auth, send_secret):
    captured = {}

    def handler(request):
        captured["authorization"] = request.headers.get("Authorization")
        captured["body"] = dict(parse_qsl(request.content.decode()))
        return httpx.Response(200, json={"access_token": "access"})

    response = _oauth_post(
        "https://idp.example.test/token",
        {"grant_type": "authorization_code", "code": "code", "client_id": "client"},
        {"client_id": "client", "token_endpoint_auth_methods_supported": [method]},
        "client-secret",
        transport=httpx.MockTransport(handler),
    )
    assert response["access_token"] == "access"
    assert bool(captured["authorization"]) is expected_auth
    assert bool(captured["body"].get("client_secret")) is send_secret
    if send_secret:
        assert captured["body"]["client_secret"] == "client-secret"
    assert ("client_id" in captured["body"]) is not expected_auth


@pytest.mark.parametrize(
    "capability,mechanism", [("AUTH=OAUTHBEARER", "OAUTHBEARER"), ("AUTH=XOAUTH2", "XOAUTH2")]
)
def test_imap_oauth_selects_advertised_mechanism_and_refreshes_once(monkeypatch, capability, mechanism):
    from emailsync.connectors import imap

    class Provider:
        def __init__(self):
            self.refreshes = 0

        def token(self, force_refresh=False, rejected_token=None):
            if force_refresh:
                self.refreshes += 1
                assert rejected_token == "old"
                return "new"
            return "old"

    class Client:
        def __init__(self, *_args, **_kwargs):
            self.normalise_times = True
            self.logins = []

        def capabilities(self):
            return [capability.encode()]

        def oauthbearer_login(self, user, token):
            self.logins.append(("OAUTHBEARER", user, token))
            if token == "old":
                raise ValueError("expired")

        def oauth2_login(self, user, token, mech="XOAUTH2", vendor=None):
            self.logins.append((mech, user, token))
            if token == "old":
                raise ValueError("expired")

        def shutdown(self):
            return None

        def logout(self):
            return None

    monkeypatch.setattr(imap, "IMAPClient", Client)
    provider = Provider()
    endpoint = Endpoint("imap", host="imap.example.test", username="alice")
    endpoint.token_provider = provider
    connection = imap.ImapConnection(endpoint).connect()
    assert connection.client.logins == [
        (mechanism, "alice", "old"),
        (mechanism, "alice", "new"),
    ]
    assert provider.refreshes == 1
    connection.close()


def test_imap_oauth_requires_supported_auth_mechanism(monkeypatch):
    from emailsync.connectors import imap

    class Client:
        def __init__(self, *_args, **_kwargs):
            self.normalise_times = True

        def capabilities(self):
            return [b"AUTH=PLAIN"]

        def shutdown(self):
            return None

    class Provider:
        def token(self, force_refresh=False):
            return "token"

    monkeypatch.setattr(imap, "IMAPClient", Client)
    endpoint = Endpoint("imap", host="imap.example.test")
    endpoint.token_provider = Provider()
    with pytest.raises(AuthError, match="server doesn't support OAuth login"):
        imap.ImapConnection(endpoint).connect()


def test_reauth_pauses_mail_task_and_clone_preserves_account_reference(tmp_path):
    from emailsync.mail.pipeline import MailMigration

    _settings, store, secretbox = _store(tmp_path)
    source = Endpoint("imap", host="imap.example.test", username="alice", password="secret")
    dest = Endpoint("jmap", url="https://mail.example.test", username="alice", password="secret")
    config = JobConfig(
        "OAuth migration",
        {
            "mail": {"source": source, "dest": dest},
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(verify=False),
    )
    job_id = store.create_job("OAuth migration", config.split()[0], None)

    class UnavailableSource:
        def connect(self):
            raise ReauthRequired("Saved")

        def close(self):
            return None

    class UnusedDestination:
        def connect(self):
            return self

        def close(self):
            return None

    result = MailMigration(
        store,
        job_id,
        config,
        RunControl(),
        source_factory=lambda *_: UnavailableSource(),
        dest_factory=lambda *_: UnusedDestination(),
    ).run()
    assert result.status == "paused"
    assert "Sign-in for account 'Saved'" in result.error
    assert store.get_job(job_id)["status"] == "queued"


def test_manager_does_not_auto_resume_paused_reauth_job(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _account(store, secretbox)
    config = JobConfig(
        "Paused OAuth",
        {
            "mail": {
                "source": Endpoint("imap", account_id=account_id),
                "dest": Endpoint("jmap", account_id=account_id),
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(),
    )
    public, secrets = config.split()
    job_id = store.create_job("Paused OAuth", public, secretbox.encrypt(secrets) if secrets else None)

    class PausedMigration:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self):
            return TaskResult(
                "paused",
                "Sign-in for account 'Saved' has expired or was revoked. "
                "Sign in again on the Accounts page, then resume the job.",
            )

    manager = JobManager(store, secretbox, _settings, mail_factory=PausedMigration)
    manager._run_job(job_id)
    assert store.get_job(job_id)["status"] == "paused"
    manager.start()
    manager.stop()
    assert store.get_job(job_id)["status"] == "paused"
    clone_id = manager.clone(job_id)
    assert store.get_job(clone_id)["config"]["endpoints"]["mail"]["source"]["account_id"] == account_id
