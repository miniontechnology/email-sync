"""Minimal JMAP client for importing messages, contacts, and calendar events."""

from urllib.parse import urlsplit, urlunsplit

import httpx

from emailsync.errors import AuthError, PermanentError, SyncError, TransientError, wrap
from emailsync.oauth import OAuthBearerAuth
from emailsync.options import Endpoint

CORE = "urn:ietf:params:jmap:core"
MAIL = "urn:ietf:params:jmap:mail"
CONTACTS = "urn:ietf:params:jmap:contacts"
CALENDARS = "urn:ietf:params:jmap:calendars"
CONTACTS_PARSE = "urn:ietf:params:jmap:contacts:parse"
CALENDARS_PARSE = "urn:ietf:params:jmap:calendars:parse"

FLAG_TO_KEYWORD = {
    "\\seen": "$seen",
    "\\flagged": "$flagged",
    "\\answered": "$answered",
    "\\draft": "$draft",
    "$forwarded": "$forwarded",
    "$junk": "$junk",
    "$notjunk": "$notjunk",
    "$phishing": "$phishing",
    "$mdnsent": "$mdnsent",
}
_KEYWORD_FORBIDDEN = set('()"{]%*\\ ')


def flags_to_keywords(flags) -> dict[str, bool]:
    out = {}
    for f in flags or ():
        lowered = str(f).lower()
        if lowered in FLAG_TO_KEYWORD:
            out[FLAG_TO_KEYWORD[lowered]] = True
        elif lowered.startswith("\\"):
            continue
        elif lowered and not (set(lowered) & _KEYWORD_FORBIDDEN) and len(lowered) <= 255:
            out[lowered] = True
    return out


def _rebase(url: str, base: str) -> str:
    """Point a session URL at the host the user configured (servers often advertise an internal name)."""
    u, b = urlsplit(url), urlsplit(base)
    return urlunsplit((b.scheme, b.netloc, u.path, u.query, u.fragment))


class JmapError(SyncError):
    pass


class JmapClient:
    def __init__(self, endpoint: Endpoint, timeout: float = 120.0):
        self.endpoint = endpoint
        base = endpoint.url.strip().rstrip("/")
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        self.base = base
        auth = None
        headers = {"Accept": "application/json"}
        if endpoint.token_provider is not None:
            auth = OAuthBearerAuth(endpoint.token_provider)
        elif endpoint.token:
            headers["Authorization"] = f"Bearer {endpoint.token}"
        else:
            auth = (endpoint.username, endpoint.password)
        self.http = httpx.Client(
            auth=auth,
            headers=headers,
            verify=not endpoint.insecure_tls,
            timeout=httpx.Timeout(timeout, connect=30.0),
            follow_redirects=True,
        )
        self.session: dict = {}
        self.account_id = ""
        self.api_url = ""
        self.upload_url = ""
        self.max_objects_in_get = 500
        self.max_calls = 16

    def connect(self, capability=MAIL) -> "JmapClient":
        try:
            r = self.http.get(self.base + "/.well-known/jmap")
            if r.status_code == 404:
                r = self.http.get(self.base + "/jmap/session")
        except Exception as exc:
            raise wrap(exc) from exc
        if r.status_code in (401, 403):
            raise AuthError(f"JMAP login failed for {self.endpoint.username} (HTTP {r.status_code})")
        try:
            r.raise_for_status()
            self.session = r.json()
        except httpx.HTTPStatusError as exc:
            raise wrap(exc) from exc
        except ValueError as exc:
            raise PermanentError(f"{self.base} did not return a JMAP session document") from exc
        accounts = self.session.get("primaryAccounts", {})
        if capability not in accounts:
            label = {MAIL: "mail", CONTACTS: "contacts", CALENDARS: "calendars"}.get(capability, capability)
            raise PermanentError(f"JMAP server does not offer {label} for this account")
        self.account_id = accounts[capability]
        self.api_url = _rebase(self.session["apiUrl"], self.base)
        self.upload_url = _rebase(self.session["uploadUrl"], self.base)
        core = self.session.get("capabilities", {}).get(CORE, {})
        self.max_objects_in_get = int(core.get("maxObjectsInGet", 500) or 500)
        self.max_calls = int(core.get("maxCallsInRequest", 16) or 16)
        return self

    def close(self) -> None:
        self.http.close()

    # -- core --------------------------------------------------------------------------------
    def call(self, calls: list[list], using=(CORE, MAIL)) -> list[list]:
        try:
            r = self.http.post(self.api_url, json={"using": list(using), "methodCalls": calls})
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPStatusError as exc:
            raise wrap(exc) from exc
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            raise TransientError(f"JMAP request failed: {exc}") from exc
        except ValueError as exc:
            raise TransientError("JMAP returned invalid JSON") from exc
        if "methodResponses" not in data:
            raise PermanentError(f"JMAP request error: {data.get('type') or data}")
        for name, args, _ in data["methodResponses"]:
            if name == "error":
                kind = args.get("type", "")
                msg = f"JMAP {kind}: {args.get('description', '')}".strip()
                if kind in ("serverUnavailable", "serverFail", "rateLimit", "tooManyRequests"):
                    raise TransientError(msg)
                raise JmapError(msg)
        return data["methodResponses"]

    def upload(self, data: bytes, content_type: str = "message/rfc822") -> str:
        url = self.upload_url.replace("{accountId}", self.account_id)
        try:
            r = self.http.post(url, content=data, headers={"Content-Type": content_type})
            r.raise_for_status()
            return r.json()["blobId"]
        except Exception as exc:
            raise wrap(exc) from exc

    # -- mailboxes ---------------------------------------------------------------------------
    def mailboxes(self) -> list[dict]:
        resp = self.call(
            [
                [
                    "Mailbox/get",
                    {
                        "accountId": self.account_id,
                        "ids": None,
                        "properties": ["id", "name", "parentId", "role"],
                    },
                    "0",
                ]
            ]
        )
        return resp[0][1]["list"]

    def create_mailbox(self, name: str, parent_id: str | None) -> str:
        resp = self.call(
            [
                [
                    "Mailbox/set",
                    {"accountId": self.account_id, "create": {"m": {"name": name, "parentId": parent_id}}},
                    "0",
                ]
            ]
        )
        args = resp[0][1]
        if "m" in (args.get("created") or {}):
            return args["created"]["m"]["id"]
        err = (args.get("notCreated") or {}).get("m", {})
        raise PermanentError(
            f"Could not create mailbox {name!r}: {err.get('type')} {err.get('description', '')}"
        )

    # -- emails ------------------------------------------------------------------------------
    def iter_email_index(self, page: int | None = None):
        """Yield every email's identity fields, paging through Email/query."""
        page = min(page or self.max_objects_in_get, self.max_objects_in_get)
        position = 0
        props = ["id", "messageId", "mailboxIds", "sentAt", "from", "to", "subject"]
        while True:
            resp = self.call(
                [
                    [
                        "Email/query",
                        {
                            "accountId": self.account_id,
                            "position": position,
                            "limit": page,
                            "sort": [{"property": "receivedAt", "isAscending": True}],
                            "collapseThreads": False,
                        },
                        "q",
                    ],
                    [
                        "Email/get",
                        {
                            "accountId": self.account_id,
                            "properties": props,
                            "#ids": {"resultOf": "q", "name": "Email/query", "path": "/ids"},
                        },
                        "g",
                    ],
                ]
            )
            ids = resp[0][1].get("ids", [])
            yield from resp[1][1].get("list", [])
            if len(ids) < page:
                return
            position += len(ids)

    def import_email(
        self, blob_id: str, mailbox_ids: list[str], keywords: dict, received_at: str | None
    ) -> str:
        email = {"blobId": blob_id, "mailboxIds": {m: True for m in mailbox_ids}, "keywords": keywords}
        if received_at:
            email["receivedAt"] = received_at
        resp = self.call([["Email/import", {"accountId": self.account_id, "emails": {"e": email}}, "0"]])
        args = resp[0][1]
        if "e" in (args.get("created") or {}):
            return args["created"]["e"]["id"]
        err = (args.get("notCreated") or {}).get("e", {})
        if err.get("type") == "alreadyExists" and err.get("existingId"):
            return err["existingId"]
        if err.get("type") in ("overQuota",):
            from emailsync.errors import FatalError

            raise FatalError(f"Destination over quota: {err.get('description', '')}")
        raise PermanentError(f"Email/import rejected: {err.get('type')} {err.get('description', '')}".strip())

    def add_to_mailboxes(self, email_id: str, mailbox_ids: list[str]) -> bool:
        patch = {f"mailboxIds/{m}": True for m in mailbox_ids}
        resp = self.call([["Email/set", {"accountId": self.account_id, "update": {email_id: patch}}, "0"]])
        return email_id in (resp[0][1].get("updated") or {})
