from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from test_protocol_migrations import STALWART_PASSWORD, STALWART_USER, _raw, _seed_greenmail

from emailsync.config import Settings
from emailsync.crypto import SecretBox
from emailsync.db import Store
from emailsync.jobs.manager import JobManager
from emailsync.options import Endpoint, JobConfig, Options
from emailsync.web.app import create_app

pytestmark = pytest.mark.integration

STALWART_URL = "http://127.0.0.1:18080"


def _callback(response, authorization_query):
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    candidates = [response.headers.get("location", "")]

    def collect(value):
        if isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, str) and ("code=" in value or value.startswith("http")):
            candidates.append(value)

    collect(payload)
    for candidate in candidates:
        if not candidate:
            continue
        params = parse_qs(urlsplit(candidate).query, keep_blank_values=True)
        if params.get("code") and params.get("state"):
            return candidate
    authorization_state = parse_qs(authorization_query).get("state", [""])[0]

    def find_code(value):
        if isinstance(value, dict):
            if value.get("code") and (value.get("state") or authorization_state):
                return {
                    "code": value["code"],
                    "state": value.get("state") or authorization_state,
                    **({"iss": value["iss"]} if value.get("iss") else {}),
                }
            for child in value.values():
                found = find_code(child)
                if found:
                    return found
        elif isinstance(value, list):
            for child in value:
                found = find_code(child)
                if found:
                    return found
        return None

    fields = find_code(payload)
    if fields:
        return "http://127.0.0.1/oauth/callback?" + urlencode(fields)
    keys = sorted(payload) if isinstance(payload, dict) else []
    location = payload.get("location", "") if isinstance(payload, dict) else ""
    parts = urlsplit(location) if isinstance(location, str) else urlsplit("")
    location_query = parse_qs(parts.query, keep_blank_values=True)
    oauth_error = {
        key: location_query[key][0] for key in ("error", "error_description") if location_query.get(key)
    }
    requested_scopes = parse_qs(authorization_query).get("scope", [])
    pytest.fail(
        f"Stalwart /api/auth response omitted a callback; response keys: {keys}; "
        f"location target: {parts.scheme}://{parts.netloc}{parts.path}; "
        f"query keys: {sorted(location_query)}; OAuth error: {oauth_error}; "
        f"requested scopes: {requested_scopes}"
    )


def _run(manager, store, secretbox, name, endpoints):
    config = JobConfig(name, endpoints, Options(workers=1, verify=False))
    public, secrets = config.split()
    job_id = store.create_job(
        name,
        public,
        secretbox.encrypt(secrets) if secrets else None,
        status="running",
    )
    manager._run_job(job_id)
    return store.get_job(job_id)


def test_stalwart_oauth_signin_then_jmap_imap_and_contacts_migrations(tmp_path):
    settings = Settings(tmp_path, "oauth-integration-key", 2, False, "INFO")
    store = Store(settings.db_path)
    secretbox = SecretBox.from_settings(settings.secret_key, settings.data_dir)
    manager = JobManager(store, secretbox, settings)
    app = create_app(settings, store, manager)
    with TestClient(app) as browser:
        created = browser.post(
            "/accounts",
            data={
                "name": "Stalwart OAuth",
                "type": "jmap",
                "url": STALWART_URL,
                "host": "127.0.0.1",
                "port": "10993",
                "security": "ssl",
                "username": STALWART_USER,
                "insecure_tls": "on",
                "auth_method": "oauth",
            },
            follow_redirects=False,
        )
        assert created.status_code == 303
        account_id = int(created.headers["location"].split("/")[-1])
        start = browser.get(f"/accounts/{account_id}/signin", follow_redirects=False)
        authorization_url = start.headers["location"]
        query = urlsplit(authorization_url).query
        authorization = parse_qs(query)
        api_response = httpx.post(
            STALWART_URL + "/api/auth",
            json={
                "type": "authCode",
                "accountName": STALWART_USER,
                "accountSecret": STALWART_PASSWORD,
                "query": query,
                "consent": True,
            },
            timeout=30,
        )
        callback = _callback(api_response, query)
        completed = browser.post("/oauth/complete", data={"callback": callback}, follow_redirects=False)
        assert completed.status_code == 303
        account = store.get_account(account_id)
        tokens = secretbox.decrypt(account["secrets_enc"])
        assert tokens.get("access_token")
        assert tokens.get("refresh_token")
        assert authorization.get("code_challenge_method") == ["S256"]

        mail_id = uuid4().hex
        greenmail = _seed_greenmail([("INBOX", _raw(f"oauth-{mail_id}@example.test", "OAuth migration"))])
        jmap = Endpoint("jmap", account_id=account_id)
        result = _run(
            manager,
            store,
            secretbox,
            "OAuth JMAP mail",
            {
                "mail": {"source": greenmail, "dest": jmap},
                "contacts": {"source": None, "dest": None},
                "calendars": {"source": None, "dest": None},
            },
        )
        assert result["status"] == "completed", result["last_error"]
        assert store.get_tasks(result["id"])[0]["copied"] == 1

        current = store.get_account(account_id)
        tokens = secretbox.decrypt(current["secrets_enc"])
        tokens["expires_at"] = 0
        store.update_account_secrets(account_id, secretbox.encrypt(tokens))
        imap_destination = Endpoint("imap", account_id=account_id)
        imap_id = uuid4().hex
        imap_source = _seed_greenmail(
            [("INBOX", _raw(f"imap-oauth-{imap_id}@example.test", "OAuth IMAP migration"))]
        )
        result = _run(
            manager,
            store,
            secretbox,
            "OAuth IMAP destination",
            {
                "mail": {"source": imap_source, "dest": imap_destination},
                "contacts": {"source": None, "dest": None},
                "calendars": {"source": None, "dest": None},
            },
        )
        assert result["status"] == "completed", result["last_error"]
        assert store.get_tasks(result["id"])[0]["copied"] == 1
        refreshed = secretbox.decrypt(store.get_account(account_id)["secrets_enc"])
        assert refreshed["expires_at"] > 0

        contact_id = uuid4().hex
        vcard_path = Path(tmp_path) / "oauth-contact.vcf"
        vcard_path.write_text(
            f"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:oauth-{contact_id}\r\n"
            f"FN:OAuth Contact {contact_id}\r\nEMAIL:oauth-{contact_id}@example.test\r\nEND:VCARD\r\n",
            encoding="utf-8",
        )
        result = _run(
            manager,
            store,
            secretbox,
            "OAuth JMAP contacts",
            {
                "mail": {"source": None, "dest": None},
                "contacts": {
                    "source": Endpoint("file", files=[str(vcard_path)]),
                    "dest": Endpoint("jmap", account_id=account_id),
                },
                "calendars": {"source": None, "dest": None},
            },
        )
        assert result["status"] == "completed", result["last_error"]
        assert store.get_tasks(result["id"])[0]["copied"] == 1
