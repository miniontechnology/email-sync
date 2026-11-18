import threading
from copy import copy
from dataclasses import dataclass

from emailsync.mail.folders import Folder
from emailsync.mail.source import MessageMeta, Scope, SourceState
from emailsync.mail.state import normalize_flags


@dataclass
class FakeMail:
    key: str
    raw: bytes
    targets: list[str]
    flags: tuple[str, ...] = ()
    size: int | None = None
    archived: bool = False
    uid: int | None = None

    def meta(self, uid):
        return MessageMeta(
            uid,
            self.key,
            list(self.targets),
            self.flags,
            size=len(self.raw) if self.size is None else self.size,
            archived=self.archived,
        )


class FakeMailState:
    def __init__(self, messages=None, gmail=False):
        self.messages = messages or {"INBOX": []}
        self.gmail = gmail
        for values in self.messages.values():
            for uid, message in enumerate(values, start=1):
                if message.uid is None:
                    message.uid = uid
        self.destination = {}
        self.dest_flags = {}
        self.destination_raw = {}
        self.raw_keys = {}
        self.upload_count = 0
        self.body_fetch_count = 0
        self.apply_failures = {}
        self.can_remove = True
        self.lock = threading.RLock()
        self.transient_fetches = 0
        self.upload_fail_after = {}
        self.drop_uploads = False


class FakeSource:
    def __init__(self, state, endpoint=None, options=None):
        self.state = state
        self.options = options
        self.gmail = state.gmail

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def scopes(self):
        return [
            Scope(
                name=name,
                target="@trash" if name.lower() == "trash" else name,
                gmail_labels=self.gmail and name.lower() == "all mail",
            )
            for name in self.state.messages
        ]

    def folders(self):
        return [
            Folder(name=name, flags=("\\Trash",) if name.lower() == "trash" else ())
            for name in self.state.messages
        ]

    def open_scope(self, scope):
        return 1, [message.uid for message in self.state.messages[scope.name]]

    def _message(self, scope, uid):
        return next(message for message in self.state.messages[scope] if message.uid == uid)

    def fetch_meta(self, scope, uids):
        result = {}
        for uid in uids:
            message = self._message(scope.name, uid)
            meta = message.meta(uid)
            if self.options and self.options.two_way and not self.gmail:
                meta.targets = ["@trash"] if scope.target == "@trash" else [scope.target]
            result[uid] = meta
        return result

    def fetch_state(self, scope, uids):
        states = {}
        for uid in uids:
            message = self._message(scope.name, uid)
            targets = list(message.targets)
            if self.options and self.options.two_way and not self.gmail:
                targets = ["@trash"] if scope.target == "@trash" else [scope.target]
            states[uid] = SourceState(tuple(message.flags), targets, message.archived)
        return states

    def fetch_bodies(self, scope, uids):
        with self.state.lock:
            self.state.body_fetch_count += 1
            if self.state.transient_fetches:
                self.state.transient_fetches -= 1
                from emailsync.errors import TransientError

                raise TransientError("temporary fake failure")
            return {uid: self._message(scope.name, uid).raw for uid in uids}

    def append(self, folder, raw, flags=(), internaldate=None):
        key = self.state.raw_keys.get(raw, f"raw:{raw.decode('utf-8', 'replace')}")
        if self.gmail:
            mailbox = next((name for name in self.state.messages if name.lower() == "all mail"), "All Mail")
            message = FakeMail(
                key,
                raw,
                [folder],
                tuple(flags),
                uid=max((item.uid or 0 for item in self.state.messages.get(mailbox, [])), default=0) + 1,
            )
            self.state.messages.setdefault(mailbox, []).append(message)
        else:
            messages = self.state.messages.setdefault(folder, [])
            messages.append(
                FakeMail(
                    key,
                    raw,
                    [folder],
                    tuple(flags),
                    uid=max((item.uid or 0 for item in messages), default=0) + 1,
                )
            )
        return len(self.state.messages[mailbox if self.gmail else folder])

    def set_flags(self, locations, add=(), remove=()):
        with self.state.lock:
            for scope, uid in locations:
                message = self._message(scope, uid)
                flags = set(normalize_flags(message.flags))
                flags.update(normalize_flags(add))
                flags.difference_update(normalize_flags(remove))
                message.flags = tuple(sorted(flags))

    def add_folder(self, location, folder):
        scope, uid = location
        message = self._message(scope, uid)
        if self.gmail:
            message.targets = list(dict.fromkeys([*message.targets, folder]))
        else:
            target = self.state.messages.setdefault(folder, [])
            if not any(item.key == message.key for item in target):
                clone = copy(message)
                clone.uid = max((item.uid or 0 for item in target), default=0) + 1
                target.append(clone)
        return True

    def remove_folder(self, location, folder):
        scope, uid = location
        message = self._message(scope, uid)
        if self.gmail:
            message.targets = [target for target in message.targets if target != folder]
            return True
        if self.state.can_remove:
            self.state.messages[scope].remove(message)
            return True
        return False

    def trash_folder(self):
        return next((name for name in self.state.messages if name.lower() == "trash"), None)

    def to_trash(self, locations):
        for scope, uid in list(locations):
            message = self._message(scope, uid)
            if self.gmail:
                message.targets = ["@trash"]
            else:
                self.state.messages[scope].remove(message)
                trash = self.state.messages.setdefault("Trash", [])
                message.uid = max((item.uid or 0 for item in trash), default=0) + 1
                trash.append(message)
        return len(locations)


class FakeDest:
    index_mode = "lazy"

    def __init__(self, state, endpoint=None, options=None):
        self.state = state
        self.options = options

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def resolve(self, target, create=True):
        if target.startswith("@") and self.options and self.options.two_way:
            target = {
                "@inbox": "INBOX",
                "@trash": "Trash",
                "@sent": "Sent",
                "@drafts": "Drafts",
                "@junk": "Junk",
                "@archive": "Archive",
            }.get(target, target)
        if create:
            self.state.destination.setdefault(target, [])
        return target, target in self.state.destination

    def index_folder(self, name, index):
        if index.is_indexed(name):
            return
        with self.state.lock:
            for uid, key in enumerate(self.state.destination.get(name, []), start=1):
                index.add(name, key)
                if self.options is None or self.options.sync_flags_labels or self.options.two_way:
                    index.add_ref(
                        key,
                        f"{name}:1:{uid}",
                        [name],
                        self.state.dest_flags.get((name, key), ()),
                    )
            index.mark_indexed(name)

    def index_all(self, index):
        for name in self.state.destination:
            self.index_folder(name, index)

    def upload(self, raw, folders, flags, internaldate, key, index):
        with self.state.lock:
            self.state.upload_count += 1
            refs = []
            remaining = self.state.upload_fail_after.get(key)
            if remaining is not None:
                if remaining <= 0:
                    self.state.upload_fail_after.pop(key, None)
                    from emailsync.errors import PermanentError

                    raise PermanentError("injected upload failure")
                self.state.upload_fail_after[key] = remaining - 1
            if not self.state.drop_uploads:
                for folder in folders:
                    messages = self.state.destination.setdefault(folder, [])
                    messages.append(key)
                    self.state.dest_flags[(folder, key)] = normalize_flags(flags)
                    self.state.destination_raw[key] = raw
                    self.state.raw_keys[raw] = key
                    refs.append(f"{folder}:1:{len(messages)}")
        return ";".join(refs) or "fake-ref"

    def _key_for_ref(self, ref):
        folder, _validity, uid = ref.rsplit(":", 2)
        return folder, int(uid), self.state.destination[folder][int(uid) - 1]

    def fetch_raw(self, ref):
        _folder, _uid, key = self._key_for_ref(ref)
        return self.state.destination_raw[key]

    def fetch_internaldate(self, _ref):
        return None

    def apply_state(self, changes, retry):
        errors = {}
        with self.state.lock:
            for change in changes:
                remaining = self.state.apply_failures.get(change.source_key, 0)
                if remaining:
                    self.state.apply_failures[change.source_key] = remaining - 1
                    errors[change.source_key] = "injected state update failure"
                    continue
                for copy, add, remove in change.flag_ops:
                    for folder in copy.folders:
                        _ref_folder, _uid, key = self._key_for_ref(copy.ref)
                        flags = set(self.state.dest_flags.get((folder, key), copy.flags))
                        flags.update(add)
                        flags.difference_update(remove)
                        self.state.dest_flags[(folder, key)] = frozenset(flags)
                source_copy = next(
                    (
                        copy
                        for copy in change.copies
                        if not (copy.folders and copy.folders <= set(change.remove_folders))
                    ),
                    change.copies[0] if change.copies else None,
                )
                if change.add_folders and source_copy:
                    source_folder, _uid, key = self._key_for_ref(source_copy.ref)
                    flags = self.state.dest_flags.get((source_folder, key), source_copy.flags)
                    for folder in change.add_folders:
                        self.state.destination.setdefault(folder, []).append(key)
                        self.state.dest_flags[(folder, key)] = flags
                for copy in change.copies:
                    for folder in change.remove_folders:
                        if folder not in copy.folders:
                            continue
                        _ref_folder, _uid, key = self._key_for_ref(copy.ref)
                        try:
                            self.state.destination[folder].remove(key)
                        except (KeyError, ValueError):
                            pass
                        if key not in self.state.destination.get(folder, []):
                            self.state.dest_flags.pop((folder, key), None)
        return errors

    def to_trash(self, copies):
        trash = self.resolve("@trash", create=True)[0]
        with self.state.lock:
            for copy_ref in copies:
                folder, _uid, key = self._key_for_ref(copy_ref.ref)
                if folder == trash:
                    continue
                try:
                    self.state.destination[folder].remove(key)
                except (KeyError, ValueError):
                    continue
                self.state.destination.setdefault(trash, []).append(key)
                self.state.dest_flags[(trash, key)] = self.state.dest_flags.get((folder, key), copy_ref.flags)
                self.state.dest_flags.pop((folder, key), None)
        return len(copies)

    @property
    def can_remove(self):
        return self.state.can_remove


def factories(state):
    return (
        lambda endpoint, options: FakeSource(state, endpoint, options),
        lambda endpoint, options: FakeDest(state, endpoint, options),
    )
