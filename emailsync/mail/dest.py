"""Mail destinations: IMAP (APPEND) and JMAP (blob upload + Email/import)."""

import datetime as dt
import threading
from collections import defaultdict

from emailsync.connectors.imap import ImapConnection
from emailsync.connectors.jmap import JmapClient, flags_to_keywords, keywords_to_flags
from emailsync.errors import AuthError, FatalError, PermanentError, wrap
from emailsync.mail.fingerprint import key_from_jmap, summary_from_headers
from emailsync.mail.folders import DEFAULT_ROLE_NAMES, Folder
from emailsync.mail.index import DestIndex
from emailsync.mail.state import StateChange, imap_flag
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

    @property
    def can_remove(self) -> bool:
        return self.conn.has("UIDPLUS")

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
        selected = self.conn.select(name, readonly=True)
        uids = self.conn.search_uids()
        for i in range(0, len(uids), INDEX_BATCH):
            for uid, data in self.conn.fetch_meta(uids[i : i + INDEX_BATCH]).items():
                key = summary_from_headers(data["header"])["key"]
                index.add(name, key)
                if self.options.sync_flags_labels:
                    index.add_ref(key, f"{name}:{selected['uidvalidity']}:{uid}", [name], data["flags"])
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
            refs.append(f"{name}:{uid}" if uid else f"{name}:?")
        return ";".join(refs)

    @staticmethod
    def _parse_ref(ref: str) -> tuple[str, int, int]:
        try:
            folder, validity, uid = ref.rsplit(":", 2)
            return folder, int(validity), int(uid)
        except (TypeError, ValueError):
            raise PermanentError("destination folder changed") from None

    def apply_state(self, changes: list[StateChange], retry) -> dict[str, str]:
        errors: dict[str, str] = {}
        flag_groups = defaultdict(list)
        for change in changes:
            for copy, add, remove in change.flag_ops:
                try:
                    folder, validity, uid = self._parse_ref(copy.ref)
                except Exception as exc:
                    errors[change.source_key] = str(wrap(exc))
                    continue
                if add:
                    flag_groups[(folder, validity, frozenset(add), True)].append((change, uid))
                if remove:
                    flag_groups[(folder, validity, frozenset(remove), False)].append((change, uid))

        for (folder, validity, flags, adding), entries in flag_groups.items():
            entries = [(change, uid) for change, uid in entries if change.source_key not in errors]
            if not entries:
                continue

            def update_flags(folder=folder, validity=validity, flags=flags, adding=adding, entries=entries):
                selected = self.conn.ensure_writable(folder)
                if selected["uidvalidity"] != validity:
                    raise PermanentError("destination folder changed")
                permanent = selected["permanentflags"]
                permitted = flags if "\\*" in permanent else flags & permanent
                imap_flags = [imap_flag(flag) for flag in sorted(permitted)]
                if not imap_flags:
                    return
                uids = sorted({uid for _change, uid in entries})
                if adding:
                    self.conn.add_flags(uids, imap_flags)
                else:
                    self.conn.remove_flags(uids, imap_flags)

            try:
                retry(update_flags)
            except (AuthError, FatalError):
                raise
            except Exception as exc:
                error = wrap(exc)
                if isinstance(error, (AuthError, FatalError)):
                    raise error from exc
                for change, _uid in entries:
                    errors[change.source_key] = str(error)

        for change in changes:
            if change.source_key in errors or not change.add_folders:
                continue
            removing = set(change.remove_folders)
            source_copy = next(
                (copy for copy in change.copies if not (copy.folders and copy.folders <= removing)),
                change.copies[0] if change.copies else None,
            )
            if source_copy is None:
                errors[change.source_key] = "No unambiguous destination copy is available"
                continue
            try:
                folder, validity, uid = self._parse_ref(source_copy.ref)
                for target in change.add_folders:
                    selected = self.conn.ensure_selected(folder)
                    if selected["uidvalidity"] != validity:
                        raise PermanentError("destination folder changed")
                    self.conn.copy([uid], target)
            except (AuthError, FatalError):
                raise
            except Exception as exc:
                error = wrap(exc)
                if isinstance(error, (AuthError, FatalError)):
                    raise error from exc
                errors[change.source_key] = str(error)

        remove_groups = defaultdict(list)
        for change in changes:
            if change.source_key in errors:
                continue
            for copy in change.copies:
                for folder in change.remove_folders:
                    if folder not in copy.folders:
                        continue
                    try:
                        source_folder, validity, uid = self._parse_ref(copy.ref)
                    except Exception as exc:
                        errors[change.source_key] = str(wrap(exc))
                        break
                    if source_folder == folder:
                        remove_groups[(folder, validity)].append((change, uid))
                if change.source_key in errors:
                    break

        for (folder, validity), entries in remove_groups.items():
            entries = [(change, uid) for change, uid in entries if change.source_key not in errors]
            if not entries:
                continue

            def remove_copies(folder=folder, validity=validity, entries=entries):
                selected = self.conn.ensure_writable(folder)
                if selected["uidvalidity"] != validity:
                    raise PermanentError("destination folder changed")
                uids = sorted({uid for _change, uid in entries})
                self.conn.add_flags(uids, ["\\Deleted"])
                self.conn.uid_expunge(uids)

            try:
                retry(remove_copies)
            except (AuthError, FatalError):
                raise
            except Exception as exc:
                error = wrap(exc)
                if isinstance(error, (AuthError, FatalError)):
                    raise error from exc
                for change, _uid in entries:
                    errors[change.source_key] = str(error)
        return errors


class JmapDest:
    index_mode = "all"
    can_remove = True

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
            index.add_ref(key, obj["id"], paths, keywords_to_flags(obj.get("keywords") or {}))
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

    def apply_state(self, changes: list[StateChange], retry) -> dict[str, str]:
        errors: dict[str, str] = {}
        patches: dict[str, dict] = {}
        sources_by_id: dict[str, set[str]] = defaultdict(set)
        for change in changes:
            change_patches: dict[str, dict] = defaultdict(dict)
            try:
                if change.add_folders and not change.copies:
                    raise PermanentError("No unambiguous destination copy is available")
                for copy, add, remove in change.flag_ops:
                    patch = change_patches[copy.ref]
                    for keyword in flags_to_keywords(add):
                        patch[f"keywords/{keyword}"] = True
                    for keyword in flags_to_keywords(remove):
                        patch[f"keywords/{keyword}"] = None
                if change.add_folders:
                    copy = change.copies[0]
                    patch = change_patches[copy.ref]
                    for mailbox_id in self._ids(change.add_folders):
                        patch[f"mailboxIds/{mailbox_id}"] = True
                for copy in change.copies:
                    remove_from = [folder for folder in change.remove_folders if folder in copy.folders]
                    if not remove_from:
                        continue
                    folder_ids = self._ids(remove_from)
                    patch = change_patches[copy.ref]
                    for mailbox_id in folder_ids:
                        patch[f"mailboxIds/{mailbox_id}"] = None
            except (AuthError, FatalError):
                raise
            except Exception as exc:
                error = wrap(exc)
                if isinstance(error, (AuthError, FatalError)):
                    raise error from exc
                errors[change.source_key] = str(error)
                continue
            for email_id, patch in change_patches.items():
                patches.setdefault(email_id, {}).update(patch)
                sources_by_id[email_id].add(change.source_key)

        ids = list(patches)
        for i in range(0, len(ids), 100):
            batch_ids = ids[i : i + 100]
            batch = {email_id: patches[email_id] for email_id in batch_ids}
            batch_sources = {email_id: sources_by_id[email_id] for email_id in batch_ids}
            try:
                not_updated = retry(lambda batch=batch: self.client.update_emails(batch))
            except (AuthError, FatalError):
                raise
            except Exception as exc:
                error = wrap(exc)
                if isinstance(error, (AuthError, FatalError)):
                    raise error from exc
                for source_keys in batch_sources.values():
                    for source_key in source_keys:
                        errors[source_key] = str(error)
                continue
            for email_id, error in not_updated.items():
                for source_key in batch_sources.get(email_id, ()):
                    errors[source_key] = error
        return errors


def make_dest(endpoint: Endpoint, options: Options):
    if endpoint.type == "jmap":
        return JmapDest(endpoint, options)
    return ImapDest(endpoint, options)
