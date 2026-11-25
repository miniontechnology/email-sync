from dataclasses import dataclass, field

from emailsync import db
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.pipeline import DavDestination, DavMigration
from emailsync.dav.source import DavItem, DavScope, DavSource
from emailsync.jobs.control import RunControl
from emailsync.options import Endpoint, JobConfig, Options


@dataclass
class State:
    items_by_scope: dict
    destination: dict
    fail_after_put: bool = False
    put_calls: int = 0
    update_calls: list = field(default_factory=list)
    update_results: list = field(default_factory=list)
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

    def items(self, scope, strict=False):
        return self.state.items_by_scope[scope.name]

    def delete(self, ref, etag):
        for items in self.state.items_by_scope.values():
            item = next((item for item in items if item.ref == ref), None)
            if item:
                if etag and item.etag != etag:
                    return False
                items.remove(item)
                return True
        return True


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

    def items(self, collection, strict=False):
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
        item = DavItem(
            filename.rsplit(".", 1)[0],
            data,
            "fingerprint:" + data.decode("utf-8", "replace"),
            ref=f"{collection['href']}/{filename}",
            etag='"etag-1"',
        )
        destination.append(item)
        if self.state.fail_after_put:
            self.state.fail_after_put = False
            from emailsync.errors import PermanentError

            raise PermanentError("simulated lost response after PUT")
        return item.ref

    def update(self, _collection, ref, etag, data):
        self.state.update_calls.append((ref, etag, data))
        result = self.state.update_results.pop(0) if self.state.update_results else True
        if isinstance(result, Exception):
            raise result
        if not result:
            return False
        item = next(item for items in self.state.destination.values() for item in items if item.ref == ref)
        item.data = data
        item.fingerprint = "fingerprint:" + data.decode("utf-8", "replace")
        item.etag = f'"etag-{len(self.state.update_calls) + 1}"'
        return True

    def get(self, ref):
        item = next(
            (item for items in self.state.destination.values() for item in items if item.ref == ref),
            None,
        )
        return (item.etag, item.data.decode()) if item else None

    def delete(self, ref, etag):
        for items in self.state.destination.values():
            item = next((item for item in items if item.ref == ref), None)
            if item:
                if item.etag != etag:
                    return False
                items.remove(item)
                return True
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


def migrate(store, job_id, kind, state, options=None, log=None):
    return DavMigration(
        store,
        job_id,
        kind,
        config(kind, options),
        RunControl(),
        source_factory=lambda ep, k: Source(state, ep, k),
        dest_factory=lambda ep, k: Destination(state, ep, k),
        log=log,
    ).run()


def test_dav_destination_items_carry_href_and_etag():
    href = "https://dav.example.test/addressbook/person.vcf"
    etag = '"etag-1"'
    data = b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nFN:Person\r\nEND:VCARD\r\n"

    class Client:
        def list_objects(self, _collection, strict=False):
            return [(href, etag, data)]

    destination = DavDestination(Endpoint("carddav", url="https://dav.example.test"), "contacts")
    destination.client = Client()

    [item] = destination.items({"href": "https://dav.example.test/addressbook/"})

    assert item.uid == "person-1"
    assert item.ref == href
    assert item.etag == etag


def test_dav_source_items_mark_multi_object_resources_ambiguous():
    href = "https://dav.example.test/addressbook/multi.vcf"
    etag = '"etag-1"'
    data = (
        "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nFN:One\r\nEND:VCARD\r\n"
        "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-2\r\nFN:Two\r\nEND:VCARD\r\n"
    )

    class Client:
        def list_objects(self, _collection, strict=False):
            return [(href, etag, data)]

    source = DavSource(Endpoint("carddav", url="https://dav.example.test"), "contacts")
    source.client = Client()

    items = source.items(DavScope("Default", {"href": "addressbook"}))

    assert {item.uid for item in items} == {"person-1", "person-2"}
    assert all(item.ref == href and item.etag == etag and item.ambiguous for item in items)


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


def _contact(uid, name, phone=True, note=""):
    lines = ["BEGIN:VCARD", "VERSION:3.0", f"UID:{uid}", f"FN:{name}"]
    if phone:
        lines.append("TEL;TYPE=WORK:+1-555-0100")
    if note:
        lines.append(f"NOTE:{note}")
    lines.append("END:VCARD")
    data = ("\r\n".join(lines) + "\r\n").encode()
    return DavItem(uid, data, f"fp:{name}")


def _baseline(row):
    import json

    return json.loads(row["info"])


def test_changed_source_contact_is_updated_once_with_new_baseline(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    changed = _contact("person-1", "One Changed", phone=False, note="Updated")
    state = State({"Takeout": [original]}, {})

    assert migrate(store, job_id, "contacts", state).status == "completed"
    first_row = store.ledger_items(job_id, "contacts", limit=None)[0]
    old_hash = _baseline(first_row)["hash"]

    state.items_by_scope["Takeout"] = [changed]
    assert migrate(store, job_id, "contacts", state).status == "completed"
    second_task = store.get_tasks(job_id)[0]
    second_row = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert state.put_calls == 1
    assert len(state.update_calls) == 1
    assert second_task["updated"] == 1
    assert (
        second_task["already"] + second_task["copied"] + second_task["skipped"] + second_task["failed"]
        == second_task["total"]
    )
    assert state.destination["default"][0].data == changed.data
    assert _baseline(second_row)["hash"] != old_hash

    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert store.get_tasks(job_id)[0]["updated"] == 0
    assert len(state.update_calls) == 1


def test_updated_contact_releases_old_fingerprint_for_later_source_item(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "Alice")
    changed = _contact("person-1", "Bob")
    new_item = _contact("person-2", "Alice")
    state = State({"Takeout": [original]}, {})
    single_worker = Options(workers=1)

    assert migrate(store, job_id, "contacts", state, single_worker).status == "completed"
    state.destination["default"][0].fingerprint = original.fingerprint
    state.items_by_scope["Takeout"] = [changed, new_item]

    result = migrate(store, job_id, "contacts", state, single_worker)

    assert result.status == "completed"
    task = store.get_tasks(job_id)[0]
    assert task["updated"] == 1
    assert task["copied"] == 1
    assert state.put_calls == 2
    assert {item.uid for item in state.destination["default"]} == {"person-1", "person-2"}


def test_legacy_ledger_info_is_upgraded_before_later_source_change(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    changed = _contact("person-1", "One Changed")
    state = State({"Takeout": [original]}, {})
    assert migrate(store, job_id, "contacts", state).status == "completed"

    row = store.ledger_items(job_id, "contacts", limit=None)[0]
    legacy_fp = _baseline(row)["fp"]
    store.ledger_mark(job_id, "contacts", row["source_key"], row["status"], info=legacy_fp)
    assert migrate(store, job_id, "contacts", state).status == "completed"
    upgraded = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert store.get_tasks(job_id)[0]["already"] == 1
    assert _baseline(upgraded)["hash"]
    assert state.update_calls == []

    state.items_by_scope["Takeout"] = [changed]
    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert len(state.update_calls) == 1
    assert store.get_tasks(job_id)[0]["updated"] == 1


def test_preseeded_item_gets_baseline_then_updates_on_later_change(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    changed = _contact("person-1", "One Changed", phone=False)
    state = State(
        {"Takeout": [original]},
        {
            "default": [
                DavItem(
                    original.uid,
                    original.data,
                    original.fingerprint,
                    ref="default/person-1.vcf",
                    etag='"preseed"',
                )
            ]
        },
    )

    assert migrate(store, job_id, "contacts", state).status == "completed"
    row = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert row["status"] == db.SKIPPED
    assert _baseline(row)["hash"]
    assert state.update_calls == []

    state.items_by_scope["Takeout"] = [changed]
    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert len(state.update_calls) == 1
    assert store.get_tasks(job_id)[0]["updated"] == 1


def test_dtstamp_only_change_does_not_update_destination(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original_data = (
        b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nDTSTAMP:20240101T000000Z\r\nFN:One\r\nEND:VCARD\r\n"
    )
    changed_data = original_data.replace(b"20240101", b"20250101")
    state = State(
        {"Takeout": [DavItem("person-1", original_data, "fp:one")]},
        {},
    )
    assert migrate(store, job_id, "contacts", state).status == "completed"
    state.items_by_scope["Takeout"] = [DavItem("person-1", changed_data, "fp:one")]

    assert migrate(store, job_id, "contacts", state).status == "completed"
    assert state.update_calls == []
    assert store.get_tasks(job_id)[0]["already"] == 1


def test_changed_item_missing_by_uid_warns_without_recreating(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    state = State({"Takeout": [original]}, {})
    assert migrate(store, job_id, "contacts", state).status == "completed"
    state.destination["default"].clear()
    state.items_by_scope["Takeout"] = [_contact("person-1", "Changed")]
    logs = []

    assert (
        migrate(
            store, job_id, "contacts", state, log=lambda level, message: logs.append((level, message))
        ).status
        == "completed"
    )
    task = store.get_tasks(job_id)[0]
    assert task["already"] == 1
    assert task["updated"] == 0
    assert state.put_calls == 1
    assert state.update_calls == []
    assert (
        "warning",
        "1 changed item(s) weren't found in the destination by UID; they weren't updated",
    ) in logs


def test_update_conflict_keeps_baseline_and_retries_on_next_run(tmp_path):
    from emailsync.errors import PermanentError

    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    changed = _contact("person-1", "Changed")
    state = State({"Takeout": [original]}, {})
    assert migrate(store, job_id, "contacts", state).status == "completed"
    row = store.ledger_items(job_id, "contacts", limit=None)[0]
    old_hash = _baseline(row)["hash"]
    state.items_by_scope["Takeout"] = [changed]
    state.update_results = [False]

    assert migrate(store, job_id, "contacts", state).status == "completed"
    conflicted = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert _baseline(conflicted)["hash"] == old_hash
    assert conflicted["error"] == "destination changed during the run; will retry"
    assert store.get_tasks(job_id)[0]["updated"] == 0

    state.update_results = [PermanentError("temporary update failure"), True]
    result = migrate(store, job_id, "contacts", state)
    assert result.status == "completed_with_errors"
    failed = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert _baseline(failed)["hash"] == old_hash
    assert store.get_tasks(job_id)[0]["failed"] == 0
    assert store.get_tasks(job_id)[0]["updated"] == 0

    assert migrate(store, job_id, "contacts", state).status == "completed"
    successful = store.ledger_items(job_id, "contacts", limit=None)[0]
    assert _baseline(successful)["hash"] != old_hash
    assert store.get_tasks(job_id)[0]["updated"] == 1


def test_update_option_off_and_dry_run_do_not_write_destination_or_ledger(tmp_path):
    store = db.Store(tmp_path / "contacts.db")
    job_id = store.create_job("contacts", {}, None)
    original = _contact("person-1", "One")
    changed = _contact("person-1", "Changed")
    state = State({"Takeout": [original]}, {})
    assert migrate(store, job_id, "contacts", state).status == "completed"
    row = store.ledger_items(job_id, "contacts", limit=None)[0]
    old_info = row["info"]
    state.items_by_scope["Takeout"] = [changed]

    assert (
        migrate(store, job_id, "contacts", state, Options(update_changed_items=False)).status == "completed"
    )
    assert store.get_tasks(job_id)[0]["updated"] == 0
    assert state.update_calls == []

    assert migrate(store, job_id, "contacts", state, Options(dry_run=True)).status == "completed"
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert store.ledger_items(job_id, "contacts", limit=None)[0]["info"] == old_info
    assert state.update_calls == []


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


def test_two_way_dav_baselines_reconciles_edits_and_pairs_new_copies(tmp_path):
    store = db.Store(tmp_path / "two-way.db")
    job_id = store.create_job("two-way contacts", {}, None)
    original = DavItem("person-1", b"BEGIN:VCARD\nUID:person-1\nFN:Before\nEND:VCARD\n", "before")
    state = State({"Default": [original]}, {})
    options = Options(two_way=True, sync_interval_minutes=15, verify=False)

    result = migrate(store, job_id, "contacts", state, options)

    assert result.status == "completed"
    assert store.get_task_baseline(job_id, "contacts") is not None
    pairs = store.dav_pairs_get_all(job_id, "contacts")
    assert pairs["Default|person-1"]["state"] == "synced"
    assert state.update_calls == []

    changed = DavItem("person-1", b"BEGIN:VCARD\nUID:person-1\nFN:After\nEND:VCARD\n", "after")
    state.items_by_scope["Default"] = [changed]
    result = migrate(store, job_id, "contacts", state, options)

    assert result.status == "completed"
    assert len(state.update_calls) == 1
    assert state.destination["default"][0].data == changed.data

    new_item = DavItem("person-2", b"BEGIN:VCARD\nUID:person-2\nFN:New\nEND:VCARD\n", "new")
    state.items_by_scope["Default"].append(new_item)
    result = migrate(store, job_id, "contacts", state, options)

    assert result.status == "completed"
    assert state.put_calls == 2
    assert "Default|person-2" in store.dav_pairs_get_all(job_id, "contacts")
    assert len(state.update_calls) == 1


def test_dav_deleted_source_item_can_be_readded_and_copied_again(tmp_path):
    store = db.Store(tmp_path / "dav-readded.db")
    job_id = store.create_job("two-way contacts", {}, None)
    original = DavItem(
        "person-1",
        b"BEGIN:VCARD\nUID:person-1\nFN:Person\nEND:VCARD\n",
        "person",
        ref="source:Default/person-1.vcf",
        etag='"source-1"',
    )
    anchor = DavItem(
        "anchor",
        b"BEGIN:VCARD\nUID:anchor\nFN:Anchor\nEND:VCARD\n",
        "anchor",
        ref="source:Default/anchor.vcf",
        etag='"source-2"',
    )
    state = State({"Default": [original, anchor]}, {})
    options = Options(two_way=True, sync_interval_minutes=15, verify=False)
    assert migrate(store, job_id, "contacts", state, options).status == "completed"

    key = "Default|person-1"
    assert store.ledger_get_many(job_id, "contacts", [key])
    state.destination["default"] = [item for item in state.destination["default"] if item.uid != "person-1"]
    assert migrate(store, job_id, "contacts", state, options).status == "completed"

    assert all(item.uid != "person-1" for item in state.items_by_scope["Default"])
    assert key not in store.dav_pairs_get_all(job_id, "contacts")
    assert store.ledger_get_many(job_id, "contacts", [key]) == {}
    assert any(item["key"] == key for item in store.dav_deleted_list(job_id, "contacts"))

    state.items_by_scope["Default"].append(original)
    previous_puts = state.put_calls
    assert migrate(store, job_id, "contacts", state, options).status == "completed"

    assert state.put_calls == previous_puts + 1
    assert any(item.uid == "person-1" for item in state.destination["default"])
    assert store.dav_pairs_get_all(job_id, "contacts")[key]["state"] == "synced"
    assert store.ledger_get_many(job_id, "contacts", [key])[key]["status"] == db.DONE
