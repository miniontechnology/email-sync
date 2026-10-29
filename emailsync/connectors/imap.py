"""Thin, retry-friendly wrapper around IMAPClient."""

import datetime as dt
import re
import ssl

from imapclient import IMAPClient
from imapclient.imap_utf7 import decode as utf7_decode
from imapclient.util import to_unicode

from emailsync.errors import AuthError, ReauthRequired, TransientError, wrap
from emailsync.mail.fingerprint import HEADER_FIELDS
from emailsync.mail.folders import Folder
from emailsync.oauth import refresh_after_rejection
from emailsync.options import Endpoint

HEADER_ITEM = ("BODY.PEEK[HEADER.FIELDS (" + " ".join(HEADER_FIELDS) + ")]").encode()
APPENDUID_RE = re.compile(rb"APPENDUID (\d+) (\d+)", re.I)
UNSETTABLE_FLAGS = {"\\recent", "\\deleted"}


def ssl_context(insecure: bool) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _str(value) -> str:
    return to_unicode(value) if isinstance(value, bytes) else str(value)


def decode_label(label) -> str:
    if isinstance(label, bytes):
        try:
            return utf7_decode(label)
        except Exception:
            return label.decode("utf-8", "replace")
    return str(label)


class ImapConnection:
    def __init__(self, endpoint: Endpoint, timeout: float = 120.0):
        self.endpoint = endpoint
        self.timeout = timeout
        self.client: IMAPClient | None = None
        self.selected: str | None = None
        self.capabilities: set[str] = set()

    # -- lifecycle ---------------------------------------------------------------------------
    def connect(self) -> "ImapConnection":
        ep = self.endpoint
        try:
            client = IMAPClient(
                ep.host,
                port=ep.effective_port(),
                ssl=ep.security == "ssl",
                ssl_context=ssl_context(ep.insecure_tls),
                timeout=self.timeout,
            )
            if ep.security == "starttls":
                client.starttls(ssl_context(ep.insecure_tls))
            client.normalise_times = False
        except Exception as exc:
            raise wrap(exc) from exc
        try:
            capabilities = {_str(c).upper() for c in client.capabilities()}
            if ep.token_provider is not None:
                if "AUTH=OAUTHBEARER" in capabilities:
                    mechanism = "OAUTHBEARER"
                elif "AUTH=XOAUTH2" in capabilities:
                    mechanism = "XOAUTH2"
                else:
                    raise AuthError("server doesn't support OAuth login")
                token = ep.token_provider.token()
                try:
                    self._oauth_login(client, ep.username, token, mechanism)
                except ReauthRequired:
                    raise
                except Exception:
                    token = refresh_after_rejection(ep.token_provider, token)
                    self._oauth_login(client, ep.username, token, mechanism)
            else:
                client.login(ep.username, ep.password)
        except ReauthRequired:
            try:
                client.shutdown()
            except Exception:
                pass
            raise
        except Exception as exc:
            try:
                client.shutdown()
            except Exception:
                pass
            if isinstance(exc, (ReauthRequired, TransientError, AuthError)):
                raise
            message = (
                f"IMAP OAuth login failed for {ep.username}@{ep.host}"
                if ep.token_provider is not None
                else f"Login failed for {ep.username}@{ep.host}: {exc}"
            )
            raise AuthError(message) from exc
        self.client = client
        self.capabilities = {_str(c).upper() for c in client.capabilities()}
        self.selected = None
        return self

    @staticmethod
    def _oauth_login(client, username, access_token, mechanism):
        if mechanism == "OAUTHBEARER":
            return client.oauthbearer_login(username, access_token)
        return client.oauth2_login(username, access_token, mech="XOAUTH2")

    def close(self) -> None:
        if self.client is None:
            return
        try:
            self.client.logout()
        except Exception:
            try:
                self.client.shutdown()
            except Exception:
                pass
        self.client = None
        self.selected = None

    def reconnect(self) -> None:
        self.close()
        self.connect()

    @property
    def c(self) -> IMAPClient:
        if self.client is None:
            self.connect()
        assert self.client is not None
        return self.client

    def has(self, capability: str) -> bool:
        return capability.upper() in self.capabilities

    # -- folders -----------------------------------------------------------------------------
    def list_folders(self) -> list[Folder]:
        out = []
        for flags, delim, name in self.c.list_folders():
            out.append(
                Folder(
                    name=_str(name),
                    delimiter=_str(delim) if delim else None,
                    flags=tuple(_str(f) for f in flags),
                )
            )
        return out

    def personal_namespace(self) -> tuple[str, str | None]:
        if not self.has("NAMESPACE"):
            return "", None
        try:
            ns = self.c.namespace()
        except Exception:
            return "", None
        if ns.personal:
            prefix, delim = ns.personal[0]
            return _str(prefix or ""), _str(delim) if delim else None
        return "", None

    def create_folder(self, name: str) -> None:
        self.c.create_folder(name)

    def select(self, name: str, readonly: bool = True) -> dict:
        info = self.c.select_folder(name, readonly=readonly)
        self.selected = name
        return {
            "uidvalidity": int(info.get(b"UIDVALIDITY", 0) or 0),
            "exists": int(info.get(b"EXISTS", 0) or 0),
            "uidnext": int(info.get(b"UIDNEXT", 0) or 0),
        }

    def ensure_selected(self, name: str) -> None:
        if self.selected != name:
            self.select(name)

    # -- messages ----------------------------------------------------------------------------
    def search_uids(self, since: dt.date | None = None) -> list[int]:
        criteria = ["SINCE", since] if since else ["ALL"]
        return sorted(int(u) for u in self.c.search(criteria))

    def fetch_meta(self, uids: list[int], gmail: bool = False) -> dict[int, dict]:
        items = [HEADER_ITEM, b"FLAGS", b"INTERNALDATE", b"RFC822.SIZE"]
        if gmail:
            items += [b"X-GM-LABELS", b"X-GM-MSGID"]
        resp = self.c.fetch(uids, items)
        out: dict[int, dict] = {}
        for uid, data in resp.items():
            header = b""
            for k, v in data.items():
                if isinstance(k, bytes) and k.upper().startswith(b"BODY[HEADER"):
                    header = v or b""
            out[int(uid)] = {
                "header": header,
                "flags": tuple(_str(f) for f in data.get(b"FLAGS", ())),
                "internaldate": data.get(b"INTERNALDATE"),
                "size": int(data.get(b"RFC822.SIZE", 0) or 0),
                "labels": [decode_label(lbl) for lbl in data.get(b"X-GM-LABELS", ())] if gmail else [],
                "gm_msgid": data.get(b"X-GM-MSGID"),
            }
        return out

    def fetch_bodies(self, uids: list[int]) -> dict[int, bytes]:
        resp = self.c.fetch(uids, [b"BODY.PEEK[]"])
        out = {}
        for uid, data in resp.items():
            body = data.get(b"BODY[]")
            if body is not None:
                out[int(uid)] = body
        return out

    def append(self, folder: str, raw: bytes, flags, internaldate) -> str | None:
        clean = [f for f in flags if f.lower() not in UNSETTABLE_FLAGS]
        resp = self.c.append(folder, raw, flags=clean, msg_time=internaldate)
        m = APPENDUID_RE.search(resp if isinstance(resp, bytes) else str(resp).encode())
        return f"{m.group(1).decode()}:{m.group(2).decode()}" if m else None
