"""Mail sources: generic IMAP and Gmail (All Mail + X-GM-LABELS)."""

import datetime as dt
from dataclasses import dataclass, field

from emailsync.connectors.imap import ImapConnection
from emailsync.errors import PermanentError
from emailsync.mail.fingerprint import summary_from_headers
from emailsync.mail.folders import Folder, gmail_targets, map_source_folder
from emailsync.options import Endpoint, Options


@dataclass
class Scope:
    name: str  # source folder name
    target: str | None = None  # fixed target for every message (generic folders); None = per message
    gmail_labels: bool = False


@dataclass
class MessageMeta:
    uid: int
    key: str
    targets: list[str]
    flags: tuple[str, ...] = ()
    internaldate: dt.datetime | None = None
    size: int = 0
    subject: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def deleted(self) -> bool:
        return any(f.lower() == "\\deleted" for f in self.flags)


class ImapSource:
    def __init__(self, endpoint: Endpoint, options: Options, gmail: bool = False):
        self.endpoint = endpoint
        self.options = options
        self.gmail = gmail
        self.conn = ImapConnection(endpoint)

    def connect(self) -> None:
        self.conn.connect()
        if self.gmail and not self.conn.has("X-GM-EXT-1"):
            raise PermanentError("Source is configured as Gmail but the server lacks X-GM-EXT-1")

    def close(self) -> None:
        self.conn.close()

    def reconnect(self) -> None:
        self.conn.reconnect()

    def folders(self) -> list[Folder]:
        return [f for f in self.conn.list_folders()]

    def scopes(self) -> list[Scope]:
        folders = [f for f in self.folders() if f.selectable]
        if not self.gmail:
            out = []
            for f in folders:
                target = map_source_folder(f, self.options)
                if target:
                    out.append(Scope(name=f.name, target=target))
            return out
        all_mail = next((f for f in folders if f.role == "all"), None)
        if all_mail is None:
            raise PermanentError(
                "Could not find Gmail's 'All Mail' folder. Enable 'Show in IMAP' for All Mail."
            )
        out = [Scope(name=all_mail.name, gmail_labels=True)]
        for f in folders:
            if f.role == "junk" and self.options.include_spam:
                out.append(Scope(name=f.name, target=self.options.folder_map.get(f.name) or "@junk"))
            if f.role == "trash" and self.options.include_trash:
                out.append(Scope(name=f.name, target=self.options.folder_map.get(f.name) or "@trash"))
        return out

    def open_scope(self, scope: Scope) -> tuple[int, list[int]]:
        info = self.conn.select(scope.name, readonly=True)
        since = dt.date.fromisoformat(self.options.since) if self.options.since else None
        return info["uidvalidity"], self.conn.search_uids(since)

    def ensure_scope(self, scope: Scope) -> None:
        self.conn.ensure_selected(scope.name)

    def fetch_meta(self, scope: Scope, uids: list[int]) -> dict[int, MessageMeta]:
        self.ensure_scope(scope)
        raw = self.conn.fetch_meta(uids, gmail=scope.gmail_labels)
        out = {}
        for uid, data in raw.items():
            summary = summary_from_headers(data["header"])
            flags = data["flags"]
            if scope.gmail_labels:
                targets, starred = gmail_targets(data["labels"], self.options)
                if starred and not any(f.lower() == "\\flagged" for f in flags):
                    flags = (*flags, "\\Flagged")
            else:
                targets = [scope.target] if scope.target else []
            out[uid] = MessageMeta(
                uid=uid,
                key=summary["key"],
                targets=targets,
                flags=tuple(flags),
                internaldate=data["internaldate"],
                size=data["size"],
                subject=summary["subject"],
                extra={"labels": data["labels"]},
            )
        return out

    def fetch_bodies(self, scope: Scope, uids: list[int]) -> dict[int, bytes]:
        self.ensure_scope(scope)
        return self.conn.fetch_bodies(uids)


def make_source(endpoint: Endpoint, options: Options):
    return ImapSource(endpoint, options, gmail=endpoint.type == "gmail")
