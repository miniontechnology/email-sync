import threading
from dataclasses import dataclass

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
    def __init__(self, messages=None):
        self.messages = messages or {"INBOX": []}
        self.destination = {}
        self.dest_flags = {}
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

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def scopes(self):
        return [Scope(name=name, target=None) for name in self.state.messages]

    def open_scope(self, scope):
        return 1, list(range(1, len(self.state.messages[scope.name]) + 1))

    def fetch_meta(self, scope, uids):
        return {uid: self.state.messages[scope.name][uid - 1].meta(uid) for uid in uids}

    def fetch_state(self, scope, uids):
        return {
            uid: SourceState(tuple(message.flags), list(message.targets), message.archived)
            for uid in uids
            if (message := self.state.messages[scope.name][uid - 1])
        }

    def fetch_bodies(self, scope, uids):
        with self.state.lock:
            self.state.body_fetch_count += 1
            if self.state.transient_fetches:
                self.state.transient_fetches -= 1
                from emailsync.errors import TransientError

                raise TransientError("temporary fake failure")
            return {uid: self.state.messages[scope.name][uid - 1].raw for uid in uids}


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
        return target, True

    def index_folder(self, name, index):
        if index.is_indexed(name):
            return
        with self.state.lock:
            for uid, key in enumerate(self.state.destination.get(name, []), start=1):
                index.add(name, key)
                if self.options is None or self.options.sync_flags_labels:
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
                    refs.append(f"{folder}:1:{len(messages)}")
        return ";".join(refs) or "fake-ref"

    def _key_for_ref(self, ref):
        folder, _validity, uid = ref.rsplit(":", 2)
        return folder, int(uid), self.state.destination[folder][int(uid) - 1]

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

    @property
    def can_remove(self):
        return self.state.can_remove


def factories(state):
    return (
        lambda endpoint, options: FakeSource(state, endpoint, options),
        lambda endpoint, options: FakeDest(state, endpoint, options),
    )
