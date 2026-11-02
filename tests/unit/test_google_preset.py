import base64
import json
import time
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient

from emailsync.accounts import check_account_compat, resolve_endpoint
from emailsync.config import Settings
from emailsync.connectors.dav import CALDAV, CARDDAV, DAV, DavClient
from emailsync.crypto import SecretBox
from emailsync.db import Store
from emailsync.errors import PermanentError
from emailsync.jobs.manager import JobManager
from emailsync.oauth import OAuthService, TokenManager
from emailsync.options import Endpoint, JobConfig, Options
from emailsync.providers import GOOGLE_CLIENT_REQUIRED, GOOGLE_IMAP, PROVIDER_PRESETS
from emailsync.web.app import create_app


def _store(tmp_path):
    settings = Settings(tmp_path, "test-key", 2, True, "INFO")
    store = Store(settings.db_path)
    secretbox = SecretBox.from_settings(settings.secret_key, settings.data_dir)
    return settings, store, secretbox


def _google_config(**overrides):
    return {
        "type": "google",
        "url": "",
        "host": "",
        "port": 0,
        "security": "ssl",
        "username": "",
        "insecure_tls": False,
        "auth_method": "oauth",
        "issuer_url": "https://accounts.google.com",
        "client_id": "google-client",
        "client_registered": False,
        "scopes": PROVIDER_PRESETS["google"]["scopes"],
        **overrides,
    }


def _google_account(store, secretbox, *, config=None, secrets=None, name="Google"):
    encrypted = secretbox.encrypt(secrets) if secrets else None
    return store.create_account(name, config or _google_config(), encrypted)


def _google_metadata():
    return {
        "issuer": "https://accounts.google.com",
        "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_endpoint": "https://oauth2.googleapis.com/token",
        "scopes_supported": ["openid", "email", "profile"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "client_secret_basic"],
        "code_challenge_methods_supported": ["S256"],
    }


def _unsigned_id_token(email):
    payload = base64.urlsafe_b64encode(json.dumps({"email": email}).encode()).decode().rstrip("=")
    return f"e30.{payload}.signature"


def test_google_account_save_normalizes_preset_and_requires_client_credentials(tmp_path):
    settings, store, secretbox = _store(tmp_path)
    with TestClient(create_app(settings, store)) as client:
        response = client.post(
            "/accounts",
            data={
                "name": "Google source",
                "type": "google",
                "url": "https://ignored.example.test",
                "host": "ignored.example.test",
                "port": "993",
                "insecure_tls": "on",
                "auth_method": "token",
                "issuer_url": "https://ignored.example.test",
                "client_id": "desktop-client",
                "client_secret": "desktop-secret",
                "scopes": "",
            },
        )
    assert response.status_code == 200
    account = store.list_accounts()[0]
    assert account["config"] == {
        "type": "google",
        "url": "",
        "host": "",
        "port": 0,
        "security": "ssl",
        "username": "",
        "insecure_tls": False,
        "auth_method": "oauth",
        "issuer_url": "https://accounts.google.com",
        "client_id": "desktop-client",
        "scopes": PROVIDER_PRESETS["google"]["scopes"],
        "client_registered": False,
    }
    assert secretbox.decrypt(account["secrets_enc"])["client_secret"] == "desktop-secret"


@pytest.mark.parametrize(
    ("client_id", "client_secret"),
    [("", "desktop-secret"), ("desktop-client", "")],
)
def test_google_account_save_requires_client_id_and_secret(tmp_path, client_id, client_secret):
    settings, store, _secretbox = _store(tmp_path)
    with TestClient(create_app(settings, store)) as client:
        response = client.post(
            "/accounts",
            data={
                "name": "Incomplete Google",
                "type": "google",
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
    assert response.status_code == 422
    assert GOOGLE_CLIENT_REQUIRED in response.text
    assert store.list_accounts() == []


def test_google_oauth_discovery_pkce_authorization_and_completion(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _google_account(
        store,
        secretbox,
        config=_google_config(scopes="", username="old-user@example.com"),
        secrets={"client_secret": "desktop-secret"},
    )
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path.endswith("oauth-authorization-server"):
            return httpx.Response(404)
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json=_google_metadata())
        if str(request.url) == "https://oauth2.googleapis.com/token":
            assert (
                request.headers["Authorization"]
                == "Basic " + base64.b64encode(b"google-client:desktop-secret").decode()
            )
            assert dict(parse_qsl(request.content.decode()))["grant_type"] == "authorization_code"
            return httpx.Response(
                200,
                json={
                    "access_token": "google-access",
                    "refresh_token": "google-refresh",
                    "expires_in": 3600,
                    "id_token": _unsigned_id_token("alice@gmail.com"),
                },
            )
        return httpx.Response(404)

    service = OAuthService(
        store,
        secretbox,
        redirect_uri="http://127.0.0.1:8080/oauth/callback",
        transport=httpx.MockTransport(handler),
    )
    authorization_url = service.start(account_id)
    parts = urlsplit(authorization_url)
    query = parse_qs(parts.query)
    assert (parts.scheme, parts.netloc, parts.path) == (
        "https",
        "accounts.google.com",
        "/o/oauth2/v2/auth",
    )
    assert query["access_type"] == ["offline"]
    assert query["prompt"] == ["consent"]
    assert query["scope"] == [PROVIDER_PRESETS["google"]["scopes"]]
    assert query["code_challenge_method"] == ["S256"]
    assert query["code_challenge"][0]
    assert store.get_account(account_id)["config"]["scopes"] == PROVIDER_PRESETS["google"]["scopes"]
    assert [str(request.url) for request in seen] == [
        "https://accounts.google.com/.well-known/oauth-authorization-server",
        "https://accounts.google.com/.well-known/openid-configuration",
    ]
    service.complete(
        "http://127.0.0.1:8080/oauth/callback?" + urlencode({"code": "auth-code", "state": query["state"][0]})
    )
    saved = store.get_account(account_id)
    secrets = secretbox.decrypt(saved["secrets_enc"])
    assert saved["config"]["username"] == "alice@gmail.com"
    assert secrets["access_token"] == "google-access"
    assert secrets["refresh_token"] == "google-refresh"


def test_google_oauth_without_client_id_raises_actionable_error(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _google_account(
        store,
        secretbox,
        config=_google_config(client_id=""),
    )
    service = OAuthService(store, secretbox, transport=httpx.MockTransport(lambda _request: None))
    with pytest.raises(PermanentError, match="Google sign-in needs your own OAuth client ID"):
        service.start(account_id)


def test_google_refresh_keeps_refresh_token_when_response_omits_it(tmp_path):
    _settings, store, secretbox = _store(tmp_path)
    account_id = _google_account(
        store,
        secretbox,
        secrets={
            "client_secret": "desktop-secret",
            "access_token": "expired-access",
            "refresh_token": "original-refresh",
            "expires_at": time.time() - 10,
        },
    )

    def handler(request):
        if request.url.path.endswith("oauth-authorization-server"):
            return httpx.Response(404)
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json=_google_metadata())
        if str(request.url) == "https://oauth2.googleapis.com/token":
            return httpx.Response(200, json={"access_token": "refreshed-access", "expires_in": 3600})
        return httpx.Response(404)

    manager = TokenManager(store, secretbox, transport=httpx.MockTransport(handler))
    assert manager.provider(account_id).token() == "refreshed-access"
    secrets = secretbox.decrypt(store.get_account(account_id)["secrets_enc"])
    assert secrets["refresh_token"] == "original-refresh"


def test_google_endpoint_resolution_and_jmap_job_rejection(tmp_path):
    settings, store, secretbox = _store(tmp_path)
    account_id = _google_account(store, secretbox)
    account = store.get_account(account_id)
    accounts = {account_id: account}

    class TokenManagerStub:
        @staticmethod
        def provider(_account_id):
            return object()

    gmail = resolve_endpoint(
        Endpoint("gmail", account_id=account_id, insecure_tls=True),
        accounts,
        TokenManagerStub(),
    )
    assert (gmail.host, gmail.port, gmail.security) == (
        GOOGLE_IMAP["host"],
        GOOGLE_IMAP["port"],
        GOOGLE_IMAP["security"],
    )
    assert gmail.token_provider is not None
    assert gmail.insecure_tls is False
    assert (
        resolve_endpoint(Endpoint("carddav", account_id=account_id), accounts, TokenManagerStub()).url
        == "https://www.googleapis.com/.well-known/carddav"
    )
    assert (
        resolve_endpoint(Endpoint("caldav", account_id=account_id), accounts, TokenManagerStub()).url
        == "https://apidata.googleusercontent.com/caldav/v2/"
    )

    jmap = Endpoint("jmap", account_id=account_id)
    message = "Account 'Google' is a Google account and can't be used for JMAP"
    assert check_account_compat(jmap, account) == message
    config = JobConfig(
        "Google to JMAP",
        {
            "mail": {
                "source": Endpoint("gmail", account_id=account_id),
                "dest": jmap,
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(),
    )
    with pytest.raises(ValueError, match="can't be used for JMAP"):
        JobManager(store, secretbox, settings).create_job(config)


def _dav_multistatus(entries):
    root = ET.Element(f"{{{DAV}}}multistatus")
    for entry in entries:
        response = ET.SubElement(root, f"{{{DAV}}}response")
        ET.SubElement(response, f"{{{DAV}}}href").text = entry["href"]
        propstat = ET.SubElement(response, f"{{{DAV}}}propstat")
        prop = ET.SubElement(propstat, f"{{{DAV}}}prop")
        if entry.get("resources"):
            resource_type = ET.SubElement(prop, f"{{{DAV}}}resourcetype")
            for namespace, name in entry["resources"]:
                ET.SubElement(resource_type, f"{{{namespace}}}{name}")
        if entry.get("displayname"):
            ET.SubElement(prop, f"{{{DAV}}}displayname").text = entry["displayname"]
        if entry.get("principal"):
            principal = ET.SubElement(prop, f"{{{DAV}}}current-user-principal")
            ET.SubElement(principal, f"{{{DAV}}}href").text = entry["principal"]
        if entry.get("home"):
            home = ET.SubElement(prop, f"{{{CALDAV}}}calendar-home-set")
            ET.SubElement(home, f"{{{DAV}}}href").text = entry["home"]
        ET.SubElement(propstat, f"{{{DAV}}}status").text = "HTTP/1.1 200 OK"
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


class OAuthProviderStub:
    @staticmethod
    def token():
        return "google-dav-access"


def _dav_client(kind, url, handler):
    endpoint = Endpoint(kind, url=url)
    endpoint.token_provider = OAuthProviderStub()
    client = DavClient(endpoint, kind=kind)
    client.http.close()
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    return client


def test_google_carddav_discovery_preserves_well_known_url_and_bearer_redirect():
    well_known = "https://www.googleapis.com/.well-known/carddav"
    collection = "https://www.googleapis.com/carddav/v1/principals/alice@gmail.com/lists/default/"
    requests = []

    def handler(request):
        requests.append(request)
        if str(request.url) == well_known:
            return httpx.Response(301, headers={"Location": collection})
        if str(request.url) == collection:
            return httpx.Response(
                207,
                content=_dav_multistatus(
                    [
                        {
                            "href": collection,
                            "resources": [(DAV, "collection"), (CARDDAV, "addressbook")],
                            "displayname": "Contacts",
                        }
                    ]
                ),
            )
        return httpx.Response(404)

    client = _dav_client("carddav", well_known, handler)
    try:
        assert client.base == well_known
        home, collections = client.discover()
    finally:
        client.close()

    assert [request.method for request in requests] == ["PROPFIND", "PROPFIND"]
    assert str(requests[0].url) == well_known
    assert requests[0].headers["Authorization"] == "Bearer google-dav-access"
    assert requests[1].headers["Authorization"] == "Bearer google-dav-access"
    assert home == collection
    assert collections == [{"href": collection, "displayname": "Contacts"}]


def test_google_caldav_discovery_follows_principal_and_lists_calendars():
    base = "https://apidata.googleusercontent.com/caldav/v2/"
    principal = "https://apidata.googleusercontent.com/principals/alice/"
    home = "https://apidata.googleusercontent.com/calendars/alice/"
    requests = []

    def handler(request):
        requests.append(request)
        if str(request.url) == base:
            return httpx.Response(
                207,
                content=_dav_multistatus([{"href": base, "principal": "/principals/alice/"}]),
            )
        if str(request.url) == principal:
            return httpx.Response(207, content=_dav_multistatus([{"href": principal, "home": home}]))
        if str(request.url) == home:
            assert request.headers["Depth"] == "1"
            return httpx.Response(
                207,
                content=_dav_multistatus(
                    [
                        {
                            "href": home + "personal/",
                            "resources": [(DAV, "collection"), (CALDAV, "calendar")],
                            "displayname": "Personal",
                        },
                        {
                            "href": home + "work/",
                            "resources": [(DAV, "collection"), (CALDAV, "calendar")],
                            "displayname": "Work",
                        },
                    ]
                ),
            )
        return httpx.Response(404)

    client = _dav_client("caldav", base, handler)
    try:
        discovered_home, collections = client.discover()
    finally:
        client.close()

    assert [str(request.url) for request in requests] == [base, principal, home]
    assert all(request.method == "PROPFIND" for request in requests)
    assert all(request.headers["Authorization"] == "Bearer google-dav-access" for request in requests)
    assert discovered_home == home
    assert collections == [
        {"href": home + "personal/", "displayname": "Personal"},
        {"href": home + "work/", "displayname": "Work"},
    ]
