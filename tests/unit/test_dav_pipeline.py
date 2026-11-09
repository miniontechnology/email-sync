from dataclasses import dataclass, field

from emailsync import db
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.pipeline import DavMigration
from emailsync.dav.source import DavItem, DavScope
from emailsync.jobs.control import RunControl
from emailsync.options import Endpoint, JobConfig, Options


@dataclass
class State:
    items_by_scope: dict
    destination: dict
    fail_after_put: bool = False
    put_calls: int = 0
    create_calls: int = 0
    item_requests: list = field(default_factory=list)
    collections: list | None = None
    home: str = "home"


class Source:
    def __init__(self, state, _endpoint, _kind):
        self.state = state

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def scopes(self):
        return [DavScope(name, {"href": name, "displayname": name}) for name in self.state.items_by_scope]

    def items(self, scope):
        return self.state.items_by_scope[scope.name]


class Destination:
    def __init__(self, state, _endpoint, _kind):
        self.state = state
        self.collections = (
            state.collections
            if state.collections is not None
            else [{"href": "default", "displayname": "Default"}]
        )
        self.home = state.home

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def items(self, collection):
        self.state.item_requests.append(collection["href"])
        return list(self.state.destination.get(collection["href"], []))

    def create_collection(self, name):
        self.state.create_calls += 1
        collection = {"href": name.lower(), "displayname": name}
        self.collections.append(collection)
        return collection

    def put(self, collection, filename, data):
        self.state.put_calls += 1
        destination = self.state.destination.setdefault(collection["href"], [])
        if any(item.data == data for item in destination):
            return False
        item = DavItem(filename.rsplit(".", 1)[0], data, "fingerprint:" + data.decode("utf-8", "replace"))
        destination.append(item)
        if self.state.fail_after_put:
            self.state.fail_after_put = False
            from emailsync.errors import PermanentError

            raise PermanentError("simulated lost response after PUT")
        return True


def config(kind="contacts", options=None):
    source_type, dest_type = ("carddav", "carddav") if kind == "contacts" else ("caldav", "caldav")
    return JobConfig(
        "DAV",
        {
            kind: {
                "source": Endpoint(source_type, url="https://source"),
                "dest": Endpoint(dest_type, url="https://dest"),
            },
            **(
                {
                    "calendars" if kind == "contacts" else "contacts": {"source": None, "dest": None},
                    "mail": {"source": None, "dest": None},
                }
            ),
        },
        options or Options(verify=False),
    )


def migrate(store, job_id, kind, state, options=None):
    return DavMigration(
        store,
        job_id,
        kind,
        config(kind, options),
        RunControl(),
        source_factory=lambda ep, k: Source(state, ep, k),
        dest_factory=lambda ep, k: Destination(state, ep, k),
    ).run()


def test_contact_copy_and_rerun_skip_by_uid(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    data = b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nFN:Person\r\nEND:VCARD\r\n"
    item = DavItem("person-1", data, "fp:person")
    state = State({"Takeout": [item]}, {})
    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert len(state.destination["default"]) == 1
    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert len(state.destination["default"]) == 1
    assert store.get_tasks(job_id)[0]["already"] == 1


def test_upload_response_loss_is_safe_to_resume(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    data = b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nFN:Person\r\nEND:VCARD\r\n"
    state = State({"Takeout": [DavItem("person-1", data, "fp:person")]}, {}, fail_after_put=True)
    assert migrate(store, job_id, "contacts", state).status == "completed_with_errors"
    assert len(state.destination["default"]) == 1
    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert len(state.destination["default"]) == 1


def test_calendar_target_matches_display_name_case_insensitively(tmp_path):
    store = db.Store(tmp_path / "calendar.db")
    job_id = store.create_job("calendar", {}, None)
    data = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n"
    scope_item = DavItem("event-1", data, "fp:event")
    state = State({"personal": [scope_item]}, {})
    assert migrate(store, job_id, "calendars", state).status == "completed"
    assert store.ledger_items(job_id, "calendars")[0]["targets"] == ["personal"]


def test_fingerprint_match_can_skip_dav_object(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    item = DavItem("new-uid", b"vcard", "same-fingerprint")
    state = State({"Takeout": [item]}, {"default": [DavItem("existing", b"existing", "same-fingerprint")]})
    result = migrate(store, job_id, "contacts", state, Options(dav_match_fingerprint=True))
    assert result.status == "completed"
    assert len(state.destination["default"]) == 1
    assert store.ledger_items(job_id, "contacts")[0]["status"] == db.SKIPPED


def test_dav_dry_run_does_not_write_and_real_run_copies_missing_items(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    copy_item = DavItem("new", b"new contact", "new-fingerprint")
    skipped_item = DavItem("existing", b"existing contact", "existing-fingerprint")
    state = State(
        {"Takeout": [copy_item, skipped_item]},
        {"default": [DavItem("existing", b"old contact", "existing-fingerprint")]},
    )

    dry_run_result = migrate(store, job_id, "contacts", state, Options(dry_run=True))
    dry_run_task = store.get_tasks(job_id)[0]
    assert dry_run_result.status == "completed"
    assert dry_run_task["copied"] == 1
    assert dry_run_task["skipped"] == 1
    assert state.put_calls == 0
    assert state.create_calls == 0
    assert store.ledger_items(job_id, "contacts", limit=None) == []
    assert len(state.destination["default"]) == 1

    real_run_result = migrate(store, job_id, "contacts", state, Options())
    real_run_task = store.get_tasks(job_id)[0]
    assert real_run_result.status == "completed"
    assert real_run_task["copied"] == 1
    assert real_run_task["skipped"] == 1
    assert state.put_calls == 1
    assert len(state.destination["default"]) == 2
    assert len(store.ledger_items(job_id, "contacts", limit=None)) == 2


def test_dav_dry_run_missing_collection_does_not_request_home(tmp_path):
    store = db.Store(tmp_path / "calendar.db")
    job_id = store.create_job("calendar", {}, None)
    item = DavItem("event-1", b"calendar event", "event-fingerprint")
    state = State({"Personal": [item]}, {}, collections=[])

    result = migrate(store, job_id, "calendars", state, Options(dry_run=True))

    assert result.status == "completed"
    assert store.get_tasks(job_id)[0]["copied"] == 1
    assert state.item_requests == []
    assert state.create_calls == 0
    assert state.put_calls == 0
    assert store.ledger_items(job_id, "calendars", limit=None) == []


def test_dav_dry_run_missing_calendars_use_independent_indexes(tmp_path):
    store = db.Store(tmp_path / "calendar.db")
    job_id = store.create_job("calendar", {}, None)
    item = DavItem("same-event", b"calendar event", "event-fingerprint")
    state = State({"Personal": [item], "Work": [item]}, {}, collections=[])

    result = migrate(store, job_id, "calendars", state, Options(dry_run=True))

    assert result.status == "completed"
    task = store.get_tasks(job_id)[0]
    assert task["copied"] == 2
    assert task["skipped"] == 0
    assert state.item_requests == []
    assert state.create_calls == 0
    assert state.put_calls == 0
    assert store.ledger_items(job_id, "calendars", limit=None) == []


def test_dav_dry_run_case_variant_calendars_share_index(tmp_path):
    store = db.Store(tmp_path / "calendar.db")
    job_id = store.create_job("calendar", {}, None)
    item = DavItem("same-event", b"calendar event", "event-fingerprint")
    state = State({"Work": [item], "work": [item]}, {}, collections=[])

    result = migrate(store, job_id, "calendars", state, Options(dry_run=True))

    assert result.status == "completed"
    task = store.get_tasks(job_id)[0]
    assert task["copied"] == 1
    assert task["skipped"] == 1
    assert state.item_requests == []
    assert state.create_calls == 0
    assert state.put_calls == 0
    assert store.ledger_items(job_id, "calendars", limit=None) == []


def test_jmap_dav_dry_run_with_no_collections_does_not_request_home(tmp_path, monkeypatch):
    calls = []

    class JmapClient:
        session = {"capabilities": {"urn:ietf:params:jmap:contacts:parse": {}}}
        account_id = "account"
        max_objects_in_get = 500

        def __init__(self, _endpoint):
            pass

        def connect(self, _capability):
            return self

        def call(self, methods, using=None):
            calls.extend(method[0] for method in methods)
            return [[methods[0][0], {"list": []}, methods[0][2]]]

        def close(self):
            pass

        def upload(self, *_args):
            raise AssertionError("dry run must not upload")

    monkeypatch.setattr("emailsync.dav.jmap_dest.JmapClient", JmapClient)
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    item = DavItem("person-1", b"contact", "contact-fingerprint")
    state = State({"Takeout": [item]}, {})
    job_config = config("contacts", Options(dry_run=True))
    job_config.endpoints["contacts"]["dest"] = Endpoint("jmap", url="https://jmap.example", username="alice")
    migration = DavMigration(
        store,
        job_id,
        "contacts",
        job_config,
        RunControl(),
        source_factory=lambda ep, kind: Source(state, ep, kind),
        dest_factory=JmapDavDestination,
    )

    result = migration.run()

    assert result.status == "completed"
    assert store.get_tasks(job_id)[0]["copied"] == 1
    assert calls == ["AddressBook/get"]
    assert store.ledger_items(job_id, "contacts", limit=None) == []
