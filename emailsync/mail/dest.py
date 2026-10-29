"""Mail destinations: IMAP (APPEND) and JMAP (blob upload + Email/import)."""

import datetime as dt
import threading

from emailsync.connectors.imap import ImapConnection
from emailsync.connectors.jmap import JmapClient, flags_to_keywords
from emailsync.errors import PermanentError
from emailsync.mail.fingerprint import key_from_jmap, summary_from_headers
from emailsync.mail.folders import DEFAULT_ROLE_NAMES, Folder
from emailsync.mail.index import DestIndex
from emailsync.options import Endpoint, Options

INDEX_BATCH = 500


def _utc_iso(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class ImapDest:
    index_mode = "lazy"

    def __init__(self, endpoint: Endpoint, options: Options):
        self.endpoint = endpoint
        self.options = options
        self.conn = ImapConnection(endpoint)
        self.folders: dict[str, Folder] = {}
        self.delimiter = "/"
        self.prefix = ""
        self._lock = threading.Lock()

    def connect(self) -> None:
        self.conn.connect()
        self.refresh_folders()
        prefix, delim = self.conn.personal_namespace()
        self.prefix = prefix
        if delim:
            self.delimiter = delim
        elif self.folders:
            self.delimiter = next((f.delimiter for f in self.folders.values() if f.delimiter), "/")

    def close(self) -> None:
        self.conn.close()

    def reconnect(self) -> None:
        self.conn.reconnect()

    def refresh_folders(self) -> None:
        self.folders = {f.name: f for f in self.conn.list_folders()}

    def _exists(self, name: str) -> str | None:
        if name.upper() == "INBOX":
            return next((n for n in self.folders if n.upper() == "INBOX"), "INBOX")
        return name if name in self.folders else None

    def _path_name(self, parts: list[str]) -> str:
        if parts[0].upper() == "INBOX" and len(parts) == 1:
            return "INBOX"
        name = self.delimiter.join(parts)
        if self.prefix and not name.startswith(self.prefix):
            name = self.prefix + name
        return name

    def resolve(self, target: str, create: bool) -> tuple[str, bool]:
        """Map a target to a real folder name. Returns (name, exists)."""
        with self._lock:
            if target.startswith("@"):
                role = target[1:]
                for f in self.folders.values():
                    if f.role == role and f.selectable:
                        return f.name, True
                name = self._path_name([DEFAULT_ROLE_NAMES.get(role, role.capitalize())])
                parts = [name]
            else:
                parts = [p for p in target.split("/") if p]
                if not parts:
                    raise PermanentError(f"Invalid destination folder {target!r}")
                name = self._path_name(parts)
            existing = self._exists(name)
            if existing:
                return existing, True
            if not create:
                return name, False
            self._create_with_parents(name)
            return name, True

    def _create_with_parents(self, name: str) -> None:
        pieces = name.split(self.delimiter)
        for i in range(1, len(pieces) + 1):
            sub = self.delimiter.join(pieces[:i])
            if self._exists(sub) or (self.prefix and sub + self.delimiter == self.prefix):
                continue
            try:
                self.conn.create_folder(sub)
            except Exception as exc:
                if "exist" not in str(exc).lower():
                    raise
        self.refresh_folders()
        if not self._exists(name):
            raise PermanentError(f"Destination folder {name!r} could not be created")

    def index_folder(self, name: str, index: DestIndex) -> None:
        if index.is_indexed(name):
            return
        if not self._exists(name):
            index.mark_indexed(name)
            return
        self.conn.select(name, readonly=True)
        uids = self.conn.search_uids()
        for i in range(0, len(uids), INDEX_BATCH):
            for data in self.conn.fetch_meta(uids[i : i + INDEX_BATCH]).values():
                index.add(name, summary_from_headers(data["header"])["key"])
        index.mark_indexed(name)

    def index_all(self, index: DestIndex) -> None:
        self.refresh_folders()
        for f in self.folders.values():
            if f.selectable:
                self.index_folder(f.name, index)

    def upload(self, raw: bytes, folders: list[str], flags, internaldate, key: str, index: DestIndex) -> str:
        refs = []
        for name in folders:
            uid = self.conn.append(name, raw, flags, internaldate)
            refs.append(f"{name}:{uid or '?'}")
        return ";".join(refs)


class JmapDest:
    index_mode = "all"

    def __init__(self, endpoint: Endpoint, options: Options):
        self.endpoint = endpoint
        self.options = options
        self.client = JmapClient(endpoint)
        self.path_to_id: dict[str, str] = {}
        self.id_to_path: dict[str, str] = {}
        self.role_to_path: dict[str, str] = {}
        self._lock = threading.Lock()

    def connect(self) -> None:
        self.client.connect()
        self.refresh_folders()

    def close(self) -> None:
        self.client.close()

    def reconnect(self) -> None:
        self.client.close()
        self.client = JmapClient(self.endpoint)
        self.connect()

    def refresh_folders(self) -> None:
        boxes = {m["id"]: m for m in self.client.mailboxes()}

        def path(mid: str, depth: int = 0) -> str:
            m = boxes[mid]
            parent = m.get("parentId")
            if parent and parent in boxes and depth < 50:
                return path(parent, depth + 1) + "/" + m["name"]
            return m["name"]

        self.path_to_id, self.id_to_path, self.role_to_path = {}, {}, {}
        for mid, m in boxes.items():
            p = path(mid)
            if m.get("role") == "inbox":
                p = "INBOX"
            self.path_to_id[p] = mid
            self.id_to_path[mid] = p
            if m.get("role"):
                self.role_to_path[m["role"]] = p

    def resolve(self, target: str, create: bool) -> tuple[str, bool]:
        with self._lock:
            if target.startswith("@"):
                role = target[1:]
                if role in self.role_to_path:
                    return self.role_to_path[role], True
                target = DEFAULT_ROLE_NAMES.get(role, role.capitalize())
            parts = [p for p in target.split("/") if p]
            if not parts:
                raise PermanentError(f"Invalid destination folder {target!r}")
            if parts[0].upper() == "INBOX":
                parts[0] = "INBOX"
            name = "/".join(parts)
            if name in self.path_to_id:
                return name, True
            if not create:
                return name, False
            parent_id = None
            for i in range(1, len(parts) + 1):
                sub = "/".join(parts[:i])
                if sub not in self.path_to_id:
                    mid = self.client.create_mailbox(parts[i - 1], parent_id)
                    self.path_to_id[sub] = mid
                    self.id_to_path[mid] = sub
                parent_id = self.path_to_id[sub]
            return name, True

    def index_folder(self, name: str, index: DestIndex) -> None:
        if not index.is_indexed(name):
            index.mark_indexed(name)

    def index_all(self, index: DestIndex) -> None:
        self.refresh_folders()
        for obj in self.client.iter_email_index():
            key = key_from_jmap(obj)
            paths = [self.id_to_path[m] for m in (obj.get("mailboxIds") or {}) if m in self.id_to_path]
            for p in paths:
                index.add(p, key)
            index.add_ref(key, obj["id"], paths)
        for p in self.path_to_id:
            index.mark_indexed(p)

    def _ids(self, folders: list[str]) -> list[str]:
        missing = [f for f in folders if f not in self.path_to_id]
        if missing:
            self.refresh_folders()
        ids = []
        for f in folders:
            if f not in self.path_to_id:
                raise PermanentError(f"Destination mailbox {f!r} does not exist")
            ids.append(self.path_to_id[f])
        return ids

    def upload(self, raw: bytes, folders: list[str], flags, internaldate, key: str, index: DestIndex) -> str:
        ids = self._ids(folders)
        existing = index.ref_for(key)
        if existing:
            email_id, in_folders = existing
            if not set(folders) <= in_folders and self.client.add_to_mailboxes(email_id, ids):
                return f"{email_id} (added to mailbox)"
        blob = self.client.upload(raw)
        return self.client.import_email(blob, ids, flags_to_keywords(flags), _utc_iso(internaldate))


def make_dest(endpoint: Endpoint, options: Options):
    if endpoint.type == "jmap":
        return JmapDest(endpoint, options)
    return ImapDest(endpoint, options)
