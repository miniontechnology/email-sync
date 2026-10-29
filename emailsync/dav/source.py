"""DAV and Takeout-file sources for contacts and calendars."""

import zipfile
from dataclasses import dataclass
from pathlib import Path

from emailsync.connectors.dav import DavClient
from emailsync.dav.vobjects import (
    contact_fingerprint,
    ensure_vcard_uid,
    event_fingerprint,
    split_ics,
    split_vcards,
    vcard_info,
)


@dataclass
class DavScope:
    name: str
    collection: dict


@dataclass
class DavItem:
    uid: str
    data: bytes
    fingerprint: str


class DavSource:
    def __init__(self, endpoint, kind):
        self.endpoint = endpoint
        self.kind = kind
        self.client = None

    def connect(self):
        if self.endpoint.type == "file":
            return self
        self.client = DavClient(self.endpoint, "carddav" if self.kind == "contacts" else "caldav")
        self.client.discover()
        return self

    def reconnect(self):
        self.close()
        return self.connect()

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

    def scopes(self):
        if self.endpoint.type == "file":
            return [
                DavScope(name=name, collection={"href": "", "displayname": name, "files": files})
                for name, files in _file_collections(self.endpoint.files, self.kind)
            ]
        _home, collections = self.client.discover()
        return [DavScope(name=item["displayname"], collection=item) for item in collections]

    def items(self, scope: DavScope):
        if self.endpoint.type == "file":
            items = []
            for _filename, data in scope.collection.get("files", []):
                items.extend(_decode_objects(data, self.kind))
            return items
        items = []
        for _href, _etag, data in self.client.list_objects(scope.collection):
            items.extend(_decode_objects(data, self.kind))
        return items


def _file_collections(paths: list[str], kind: str):
    wanted = ".vcf" if kind == "contacts" else ".ics"
    for path in paths:
        source = Path(path)
        if source.suffix.lower() == ".zip":
            with zipfile.ZipFile(source) as archive:
                grouped = {}
                for name in archive.namelist():
                    if Path(name).suffix.lower() == wanted:
                        grouped.setdefault(Path(name).stem, []).append((Path(name).name, archive.read(name)))
                yield from grouped.items()
        elif source.suffix.lower() == wanted:
            yield source.stem, [(source.name, source.read_bytes())]


def _decode_objects(data: str | bytes, kind: str):
    if isinstance(data, bytes):
        text = data.decode("utf-8", "replace")
    else:
        text = data
    if kind == "contacts":
        for card in split_vcards(text):
            uid, card = ensure_vcard_uid(card)
            _uid, fn, emails = vcard_info(card)
            yield DavItem(uid, card.encode("utf-8"), contact_fingerprint(fn, emails))
        return
    for uid, calendar in split_ics(text):
        from icalendar import Calendar

        parsed = Calendar.from_ical(calendar)
        components = [c for c in parsed.subcomponents if c.name in ("VEVENT", "VTODO", "VJOURNAL")]
        component = components[0] if components else None
        summary = str(component.get("summary", "")) if component else ""
        start = component.get("dtstart") if component else None
        yield DavItem(uid, calendar, event_fingerprint(summary, start))
