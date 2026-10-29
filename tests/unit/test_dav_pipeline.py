from dataclasses import dataclass

from emailsync import db
from emailsync.dav.pipeline import DavMigration
from emailsync.dav.source import DavItem, DavScope
from emailsync.jobs.control import RunControl
from emailsync.options import Endpoint, JobConfig, Options


@dataclass
class State:
    items_by_scope: dict
    destination: dict
    fail_after_put: bool = False


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
        self.collections = [{"href": "default", "displayname": "Default"}]
        self.home = "home"

    def connect(self):
        return self

    def reconnect(self):
        return self

    def close(self):
        pass

    def items(self, collection):
        return list(self.state.destination.get(collection["href"], []))

    def create_collection(self, name):
        collection = {"href": name.lower(), "displayname": name}
        self.collections.append(collection)
        return collection

    def put(self, collection, filename, data):
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
