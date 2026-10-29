import threading
from dataclasses import dataclass

from emailsync.mail.source import MessageMeta, Scope


@dataclass
class FakeMail:
    key: str
    raw: bytes
    targets: list[str]
    flags: tuple[str, ...] = ()
    size: int | None = None

    def meta(self, uid):
        return MessageMeta(
            uid,
            self.key,
            list(self.targets),
            self.flags,
            size=len(self.raw) if self.size is None else self.size,
        )


class FakeMailState:
    def __init__(self, messages=None):
        self.messages = messages or {"INBOX": []}
        self.destination = {}
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

    def fetch_bodies(self, scope, uids):
        with self.state.lock:
            if self.state.transient_fetches:
                self.state.transient_fetches -= 1
                from emailsync.errors import TransientError

                raise TransientError("temporary fake failure")
            return {uid: self.state.messages[scope.name][uid - 1].raw for uid in uids}


class FakeDest:
    index_mode = "lazy"

    def __init__(self, state, endpoint=None, options=None):
        self.state = state

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def resolve(self, target, create=True):
        return target, True

    def index_folder(self, name, index):
        with self.state.lock:
            for key in self.state.destination.get(name, []):
                index.add(name, key)
            index.mark_indexed(name)

    def index_all(self, index):
        for name in self.state.destination:
            self.index_folder(name, index)

    def upload(self, raw, folders, flags, internaldate, key, index):
        with self.state.lock:
            remaining = self.state.upload_fail_after.get(key)
            if remaining is not None:
                if remaining <= 0:
                    self.state.upload_fail_after.pop(key, None)
                    from emailsync.errors import PermanentError

                    raise PermanentError("injected upload failure")
                self.state.upload_fail_after[key] = remaining - 1
            if not self.state.drop_uploads:
                for folder in folders:
                    self.state.destination.setdefault(folder, []).append(key)
        return "fake-ref"


def factories(state):
    return (
        lambda endpoint, options: FakeSource(state, endpoint, options),
        lambda endpoint, options: FakeDest(state, endpoint, options),
    )
