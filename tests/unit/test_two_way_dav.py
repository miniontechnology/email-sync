import json
import sqlite3

from emailsync import db
from emailsync.dav.source import DavItem, DavScope, _decode_objects
from emailsync.dav.twoway import DavTwoWayReconciler
from emailsync.dav.vobjects import content_hash
from emailsync.errors import PermanentError
from emailsync.options import Endpoint, JobConfig, Options


def _config(
    *,
    mail_source=None,
    mail_dest=None,
    contacts_source=None,
    contacts_dest=None,
    calendars_source=None,
    calendars_dest=None,
):
    return JobConfig(
        "Two-way DAV",
        {
            "mail": {"source": mail_source, "dest": mail_dest},
            "contacts": {"source": contacts_source, "dest": contacts_dest},
            "calendars": {"source": calendars_source, "dest": calendars_dest},
        },
        Options(two_way=True, sync_interval_minutes=15),
    )


def _contact(uid, name, rev="20240101T000000Z"):
    data = (f"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:{uid}\r\nFN:{name}\r\nREV:{rev}\r\nEND:VCARD\r\n").encode()
    return DavItem(uid, data, f"contact:{name}")


def _event(uid, summary, last_modified="20240101T000000Z", second_modified=None):
    components = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        "DTSTAMP:20240101T000000Z",
        f"LAST-MODIFIED:{last_modified}",
        f"SUMMARY:{summary}",
        "DTSTART:20240102T120000Z",
        "END:VEVENT",
    ]
    if second_modified:
        components.extend(
            [
                "BEGIN:VTODO",
                f"UID:{uid}",
                f"LAST-MODIFIED:{second_modified}",
                "SUMMARY:Other component",
                "END:VTODO",
            ]
        )
    data = ("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + "\r\n".join(components) + "\r\nEND:VCALENDAR\r\n").encode()
    return DavItem(uid, data, f"event:{summary}")


def _with_ref(item, ref, etag):
    return DavItem(item.uid, item.data, item.fingerprint, ref, etag, item.ambiguous)


class _FakeSource:
    def __init__(
        self, kind, items_by_scope, scopes=None, put_results=None, update_results=None, delete_results=None
    ):
        self.kind = kind
        self.scopes_list = scopes or [
            DavScope(name, {"href": f"source:{name}", "displayname": name}) for name in items_by_scope
        ]
        self.items_by_scope = items_by_scope
        self.strict_failures = set()
        self.put_results = list(put_results or [])
        self.update_results = list(update_results or [])
        self.delete_results = list(delete_results or [])
        self.put_calls = []
        self.update_calls = []
        self.delete_calls = []

    def scopes(self):
        return list(self.scopes_list)

    def items(self, scope, strict=False):
        if strict and scope.name in self.strict_failures:
            raise RuntimeError("incomplete DAV listing")
        return list(self.items_by_scope.get(scope.name, []))

    def _find(self, ref):
        for items in self.items_by_scope.values():
            for item in items:
                if item.ref == ref:
                    return items, item
        return None, None

    def put(self, collection, filename, data):
        self.put_calls.append((collection, filename, data))
        result = self.put_results.pop(0) if self.put_results else None
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        decoded = list(_decode_objects(data, self.kind))
        name = collection.get("displayname", collection["href"])
        scope_items = self.items_by_scope.setdefault(name, [])
        if any(item.uid == decoded[0].uid for item in scope_items):
            return False
        href = f"{collection['href'].rstrip('/')}/{filename}"
        scope_items.append(_with_ref(decoded[0], href, '"source-new"'))
        return href

    def update(self, _collection, ref, etag, data):
        self.update_calls.append((ref, etag, data))
        result = self.update_results.pop(0) if self.update_results else True
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        items, item = self._find(ref)
        if item is None or item.etag != etag:
            return False
        decoded = next(iter(_decode_objects(data, self.kind)))
        items[items.index(item)] = _with_ref(decoded, ref, '"source-updated"')
        return True

    def get(self, ref):
        _items, item = self._find(ref)
        return (item.etag, item.data.decode()) if item else None

    def delete(self, ref, etag):
        self.delete_calls.append((ref, etag))
        result = self.delete_results.pop(0) if self.delete_results else True
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        items, item = self._find(ref)
        if item is None:
            return True
        if item.etag != etag:
            return False
        items.remove(item)
        return True


class _FakeDest:
    def __init__(
        self, kind, destination, collections=None, put_results=None, update_results=None, delete_results=None
    ):
        self.kind = kind
        self.destination = destination
        self.collections = collections or [{"href": "dest:Default", "displayname": "Default"}]
        self.put_results = list(put_results or [])
        self.update_results = list(update_results or [])
        self.delete_results = list(delete_results or [])
        self.put_calls = []
        self.update_calls = []
        self.delete_calls = []

    def items(self, collection, strict=False):
        return list(self.destination.get(collection["href"], []))

    def _find(self, ref):
        for items in self.destination.values():
            for item in items:
                if item.ref == ref:
                    return items, item
        return None, None

    def put(self, collection, filename, data):
        self.put_calls.append((collection, filename, data))
        result = self.put_results.pop(0) if self.put_results else None
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        decoded = list(_decode_objects(data, self.kind))
        items = self.destination.setdefault(collection["href"], [])
        if any(item.uid == decoded[0].uid for item in items):
            return False
        href = f"{collection['href'].rstrip('/')}/{filename}"
        items.append(_with_ref(decoded[0], href, '"dest-new"'))
        return href

    def update(self, _collection, ref, etag, data):
        self.update_calls.append((ref, etag, data))
        result = self.update_results.pop(0) if self.update_results else True
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        items, item = self._find(ref)
        if item is None or item.etag != etag:
            return False
        decoded = next(iter(_decode_objects(data, self.kind)))
        items[items.index(item)] = _with_ref(decoded, ref, '"dest-updated"')
        return True

    def get(self, ref):
        _items, item = self._find(ref)
        return (item.etag, item.data.decode()) if item else None

    def delete(self, ref, etag):
        self.delete_calls.append((ref, etag))
        result = self.delete_results.pop(0) if self.delete_results else True
        if isinstance(result, Exception):
            raise result
        if result is False:
            return False
        items, item = self._find(ref)
        if item is None:
            return True
        if item.etag != etag:
            return False
        items.remove(item)
        return True


class _Progress:
    def __init__(self):
        self.values = {}

    def update(self, scope, field, amount=1, status=None):
        self.values[field] = self.values.get(field, 0) + amount

    def increment_task(self, field, amount=1):
        self.values[field] = self.values.get(field, 0) + amount


def _reconciler(
    store,
    job_id,
    kind,
    items_by_scope,
    destination,
    *,
    scopes=None,
    options=None,
    logs=None,
    source=None,
    dest=None,
    retry=None,
    control=None,
):
    source = source or _FakeSource(kind, items_by_scope, scopes=scopes)
    dest = dest or _FakeDest(kind, destination)
    log_entries = logs if logs is not None else []
    progress = _Progress()
    reconciler = DavTwoWayReconciler(
        store,
        job_id,
        kind,
        source,
        dest,
        options or Options(max_deletions=50, max_deletion_percent=10),
        retry or (lambda operation, _connection: operation()),
        progress,
        lambda level, message: log_entries.append((level, message)),
        lambda _error: None,
        control=control,
    )
    return reconciler, source, dest, progress, log_entries


def _skip_retry_call(connection, call_number):
    calls = 0

    def retry(operation, current_connection):
        nonlocal calls
        if current_connection is connection:
            calls += 1
            if calls == call_number:
                return None
        return operation()

    return retry


def test_two_way_kinds_include_only_supported_pairs():
    contacts = _config(
        contacts_source=Endpoint("carddav", url="https://source", username="alice"),
        contacts_dest=Endpoint("carddav", url="https://dest", username="alice"),
        calendars_source=Endpoint("caldav", url="https://source", username="alice"),
        calendars_dest=Endpoint("jmap", url="https://dest", username="alice"),
    )

    assert contacts.two_way_kinds() == ["contacts"]
    assert not any("needs mail" in error for error in contacts.validate())

    unsupported = _config(
        contacts_source=Endpoint("carddav", url="https://source", username="alice"),
        contacts_dest=Endpoint("jmap", url="https://dest", username="alice"),
    )
    errors = unsupported.validate()
    assert unsupported.two_way_kinds() == []
    assert "Two-way sync needs mail, or contacts/calendars over CardDAV/CalDAV on both sides." in errors


def test_two_way_kinds_keep_mail_type_errors_only_when_mail_is_enabled():
    config = _config(
        mail_source=Endpoint("file", files=["mail.mbox"]),
        mail_dest=Endpoint("jmap", url="https://dest", username="alice"),
    )

    errors = config.validate()

    assert "Two-way sync requires an IMAP or Gmail mail source." in errors
    assert "Two-way sync needs mail, or contacts/calendars over CardDAV/CalDAV on both sides." in errors


def test_dav_pair_deleted_record_and_task_baseline_storage(tmp_path):
    store = db.Store(tmp_path / "two-way-dav.db")
    job_id = store.create_job("DAV", {}, None)
    store.reset_task(job_id, "contacts", "running")
    store.set_task_baseline(job_id, "contacts", 123.5)

    store.reset_task(job_id, "contacts", "running")

    assert store.get_task_baseline(job_id, "contacts") == 123.5
    store.dav_pairs_put_many(
        job_id,
        "contacts",
        [
            {
                "key": "Default|contact-1",
                "state": "synced",
                "src_hash": "source-hash",
                "dst_hash": "dest-hash",
            }
        ],
    )
    assert store.dav_pairs_get_all(job_id, "contacts")["Default|contact-1"]["dst_hash"] == "dest-hash"
    store.dav_pairs_delete(job_id, "contacts", ["Default|contact-1"])
    assert store.dav_pairs_get_all(job_id, "contacts") == {}

    deleted_id = store.dav_deleted_add(
        job_id, "contacts", "Default|contact-1", "source", "Default", b"saved vCard"
    )

    [deleted] = store.dav_deleted_list(job_id, "contacts")
    assert deleted["id"] == deleted_id
    assert deleted["data"] == b"saved vCard"
    assert store.dav_deleted_count(job_id) == {"contacts": 1}


def test_kind_scoped_pending_and_approved_deletions_preserve_legacy_mail(tmp_path):
    store = db.Store(tmp_path / "deletions.db")
    job_id = store.create_job("DAV", {}, None)
    store.set_pending_deletions(job_id, "mail", [{"key": "mail-1", "side": "source"}])
    store.set_pending_deletions(job_id, "contacts", [{"key": "contact-1", "side": "destination"}])

    approved = store.approve_pending_deletions(job_id)

    assert {item["kind"] for item in approved} == {"mail", "contacts"}
    assert store.get_approved(job_id, "mail") == {("mail-1", "source")}
    assert store.get_approved(job_id, "contacts") == {("contact-1", "destination")}
    assert store.get_job(job_id)["approved_deletions"] == [
        {"kind": "mail", "key": "mail-1", "side": "source"},
        {"kind": "contacts", "key": "contact-1", "side": "destination"},
    ]

    store.update_job(
        job_id,
        pending_deletions=json.dumps(
            [{"key": "legacy-mail", "side": "source"}, {"kind": "contacts", "key": "contact-2"}]
        ),
        approved_deletions=json.dumps(["legacy-approved"]),
    )
    assert store.get_pending_deletions(job_id, "mail") == [
        {"key": "legacy-mail", "side": "source", "kind": "mail"}
    ]
    assert store.get_approved(job_id, "mail") == set()
    assert store.get_approved(job_id, "contacts") == set()

    store.set_pending_deletions(job_id, "mail", [{"key": "replacement"}])
    store.clear_deletions(job_id, "mail")

    assert store.get_pending_deletions(job_id, "mail") == []
    assert store.get_pending_deletions(job_id, "contacts") == [{"kind": "contacts", "key": "contact-2"}]
    assert store.get_approved(job_id, "mail") == set()


def test_ledger_reset_clears_dav_pairs_baseline_and_deletions_by_kind(tmp_path):
    store = db.Store(tmp_path / "reset.db")
    job_id = store.create_job("DAV", {}, None)
    store.reset_task(job_id, "contacts", "running")
    store.reset_task(job_id, "calendars", "running")
    store.set_task_baseline(job_id, "contacts", 10)
    store.set_task_baseline(job_id, "calendars", 20)
    store.dav_pairs_put_many(
        job_id,
        "contacts",
        [{"key": "book|contact", "state": "synced", "src_hash": "a", "dst_hash": "b"}],
    )
    store.dav_pairs_put_many(
        job_id,
        "calendars",
        [{"key": "calendar|event", "state": "synced", "src_hash": "a", "dst_hash": "b"}],
    )
    store.set_pending_deletions(job_id, "contacts", [{"key": "contact"}])
    store.set_pending_deletions(job_id, "calendars", [{"key": "event"}])

    store.ledger_reset(job_id, "contacts")

    assert store.dav_pairs_get_all(job_id, "contacts") == {}
    assert store.dav_pairs_get_all(job_id, "calendars")
    assert store.get_task_baseline(job_id, "contacts") is None
    assert store.get_task_baseline(job_id, "calendars") == 20
    assert store.get_pending_deletions(job_id, "contacts") == []
    assert store.get_pending_deletions(job_id, "calendars")[0]["key"] == "event"


def test_task_baseline_column_migrates_existing_tasks_table(tmp_path):
    path = tmp_path / "old.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY)")
    connection.execute(
        "CREATE TABLE tasks (job_id INTEGER NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL,"
        " PRIMARY KEY(job_id, kind))"
    )
    connection.execute("INSERT INTO jobs(id) VALUES(1)")
    connection.commit()
    connection.close()

    store = db.Store(path)
    columns = {row["name"] for row in store.query("PRAGMA table_info(tasks)")}

    assert "twoway_baseline_at" in columns
    store.set_task_baseline(1, "contacts", 42)
    assert store.get_task_baseline(1, "contacts") == 42


def test_dav_baseline_records_pairs_and_ignores_destination_only_items(tmp_path):
    store = db.Store(tmp_path / "baseline.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    destination_item = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    extra_item = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d2"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [source_item]},
        {"dest:Default": [destination_item, extra_item]},
    )

    assert reconciler.reconcile(baseline=True) is False

    pairs = store.dav_pairs_get_all(job_id, "contacts")
    assert pairs["Default|person-1"]["state"] == "synced"
    assert pairs["Default|person-1"]["src_hash"] == pairs["Default|person-1"]["dst_hash"]
    assert pairs["Default|dest-only"]["state"] == "ignored"
    assert source.put_calls == dest.put_calls == []
    assert source.delete_calls == dest.delete_calls == []
    assert store.get_task_baseline(job_id, "contacts") is not None


def test_incomplete_snapshot_retries_dav_baseline_next_run(tmp_path):
    store = db.Store(tmp_path / "incomplete-baseline.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    destination_item = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, _dest, _progress, logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [source_item]},
        {"dest:Default": [destination_item]},
    )
    source.strict_failures.add("Default")

    assert reconciler.reconcile(baseline=True) is False
    assert store.dav_pairs_get_all(job_id, "contacts") == {}
    assert store.get_task_baseline(job_id, "contacts") is None
    assert any(
        level == "warning" and "baseline was not recorded" in message and "retried next run" in message
        for level, message in logs
    )

    source.strict_failures.clear()
    assert reconciler.reconcile(baseline=True) is False

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    assert pair["state"] == "synced"
    assert store.get_task_baseline(job_id, "contacts") is not None


def test_removed_ignored_collection_does_not_guard_other_deletions(tmp_path):
    store = db.Store(tmp_path / "ignored-collection-removed.db")
    job_id = store.create_job("DAV", {}, None)
    work_one = _with_ref(_event("event-1", "One"), "source:Work/event-1.ics", '"s1"')
    work_two = _with_ref(_event("event-2", "Two"), "source:Work/event-2.ics", '"s2"')
    destination_one = _with_ref(_event("event-1", "One"), "dest:Work/event-1.ics", '"d1"')
    destination_two = _with_ref(_event("event-2", "Two"), "dest:Work/event-2.ics", '"d2"')
    ignored_item = _with_ref(_event("ignored-only", "Extra"), "dest:Ignored/extra.ics", '"d3"')
    scopes = [
        DavScope("Work", {"href": "source:Work", "displayname": "Work"}),
        DavScope("Ignored", {"href": "source:Ignored", "displayname": "Ignored"}),
    ]
    collections = [
        {"href": "dest:Work", "displayname": "Work"},
        {"href": "dest:Ignored", "displayname": "Ignored"},
    ]
    reconciler, source, dest, _progress, logs = _reconciler(
        store,
        job_id,
        "calendars",
        {"Work": [work_one, work_two], "Ignored": []},
        {"dest:Work": [destination_one, destination_two], "dest:Ignored": [ignored_item]},
        scopes=scopes,
        dest=_FakeDest(
            "calendars",
            {"dest:Work": [destination_one, destination_two], "dest:Ignored": [ignored_item]},
            collections=collections,
        ),
    )

    reconciler.reconcile(baseline=True)
    assert store.dav_pairs_get_all(job_id, "calendars")["Ignored|ignored-only"]["state"] == "ignored"

    dest.collections = [collections[0]]
    dest.destination.pop("dest:Ignored")
    source.items_by_scope["Work"] = [work_two]

    reconciler.reconcile()

    assert dest.delete_calls == [(destination_one.ref, destination_one.etag)]
    pairs = store.dav_pairs_get_all(job_id, "calendars")
    assert "Work|event-1" not in pairs
    assert pairs["Ignored|ignored-only"]["state"] == "ignored"
    assert not any("deletions were suppressed" in message for _level, message in logs)


def test_pairs_keep_their_destination_collection_when_default_is_added(tmp_path):
    store = db.Store(tmp_path / "default-added.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    original_copy = _with_ref(_contact("person-1", "One"), "dest:Contacts/person-1.vcf", '"d1"')
    reconciler, _source, dest, progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [source_item]},
        {"dest:Contacts": [original_copy]},
        dest=_FakeDest(
            "contacts",
            {"dest:Contacts": [original_copy]},
            collections=[{"href": "dest:Contacts", "displayname": "Contacts"}],
        ),
    )

    reconciler.reconcile(baseline=True)
    assert store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]["dst_href"] == "dest:Contacts"

    new_default_copy = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d2"')
    dest.collections.append({"href": "dest:Default", "displayname": "Default"})
    dest.destination["dest:Default"] = [new_default_copy]

    assert reconciler.reconcile() is False

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    assert pair["dst_href"] == "dest:Contacts"
    assert dest.update_calls == []
    assert dest.delete_calls == []
    assert _source.put_calls == []
    assert store.get_pending_deletions(job_id, "contacts") == []
    assert progress.values.get("updated", 0) == 0


def test_dav_source_and_destination_edits_propagate_then_settle(tmp_path):
    store = db.Store(tmp_path / "edits.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(_contact("person-1", "Original"), "source:Default/person-1.vcf", '"s1"')
    destination_item = _with_ref(_contact("person-1", "Original"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [source_item]},
        {"dest:Default": [destination_item]},
    )
    reconciler.reconcile(baseline=True)

    source.items_by_scope["Default"][0] = _with_ref(
        _contact("person-1", "Source edit", "20240201T000000Z"), source_item.ref, source_item.etag
    )
    reconciler.reconcile()
    assert next(iter(dest.destination["dest:Default"])).data == source.items_by_scope["Default"][0].data

    current_destination = dest.destination["dest:Default"][0]
    dest.destination["dest:Default"][0] = _with_ref(
        _contact("person-1", "Destination edit", "20240301T000000Z"),
        current_destination.ref,
        current_destination.etag,
    )
    reconciler.reconcile()

    assert source.items_by_scope["Default"][0].data == dest.destination["dest:Default"][0].data
    assert len(dest.update_calls) == 1
    assert len(source.update_calls) == 1
    assert progress.values["updated"] == 2

    reconciler.reconcile()

    assert len(dest.update_calls) == 1
    assert len(source.update_calls) == 1


def test_reformatted_ics_readback_does_not_create_a_false_edit(tmp_path):
    store = db.Store(tmp_path / "reformatted-readback.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(
        _event("event-1", "Original"),
        "source:Calendar/event-1.ics",
        '"s1"',
    )
    dst = _with_ref(
        _event("event-1", "Original"),
        "dest:Calendar/event-1.ics",
        '"d1"',
    )
    source = _FakeSource(
        "calendars",
        {"Calendar": [src]},
        scopes=[DavScope("Calendar", {"href": "source:Calendar", "displayname": "Calendar"})],
    )
    dest = _FakeDest(
        "calendars",
        {"dest:Calendar": [dst]},
        collections=[{"href": "dest:Calendar", "displayname": "Calendar"}],
    )
    reconciler, source, dest, _progress, _logs = _reconciler(
        store, job_id, "calendars", source.items_by_scope, dest.destination, source=source, dest=dest
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Calendar"][0] = _with_ref(
        _event("event-1", "Source edit", "20240201T000000Z"),
        src.ref,
        src.etag,
    )
    get = dest.get

    def reformatted_get(ref):
        result = get(ref)
        if result:
            etag, data = result
            return etag, data.replace("SUMMARY:Source edit", "SUMMARY:Source\r\n  edit")
        return result

    dest.get = reformatted_get

    reconciler.reconcile()

    pair = store.dav_pairs_get_all(job_id, "calendars")["Calendar|event-1"]
    assert pair["dst_hash"] == content_hash(dest.destination["dest:Calendar"][0].data)
    assert len(dest.update_calls) == 1

    reconciler.reconcile()

    assert len(dest.update_calls) == 1
    assert source.update_calls == []


def test_dav_conflict_uses_rev_and_source_wins_when_missing_or_equal(tmp_path):
    for index, (src_rev, dst_rev, expected) in enumerate(
        (
            ("20240201T000000Z", "20240301T000000Z", "destination"),
            ("", "20240301T000000Z", "source"),
            ("20240301T000000Z", "20240301T000000Z", "source"),
        )
    ):
        store = db.Store(tmp_path / f"conflict-{index}.db")
        job_id = store.create_job("DAV", {}, None)
        src = _with_ref(_contact("person-1", "Original"), "source:Default/person-1.vcf", '"s1"')
        dst = _with_ref(_contact("person-1", "Original"), "dest:Default/person-1.vcf", '"d1"')
        reconciler, source, dest, _progress, _logs = _reconciler(
            store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
        )
        reconciler.reconcile(baseline=True)
        source.items_by_scope["Default"][0] = _with_ref(
            _contact("person-1", "Source edit", src_rev), src.ref, src.etag
        )
        dest.destination["dest:Default"][0] = _with_ref(
            _contact("person-1", "Destination edit", dst_rev), dst.ref, dst.etag
        )

        reconciler.reconcile()

        if expected == "destination":
            assert source.items_by_scope["Default"][0].data == dest.destination["dest:Default"][0].data
            assert len(source.update_calls) == 1
            assert not dest.update_calls
        else:
            assert dest.destination["dest:Default"][0].data == source.items_by_scope["Default"][0].data
            assert len(dest.update_calls) == 1
            assert not source.update_calls


def test_ics_conflict_uses_max_last_modified_not_dtstamp(tmp_path):
    store = db.Store(tmp_path / "ics-conflict.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(
        _event("event-1", "Original", "20240101T000000Z"),
        "source:Calendar/event-1.ics",
        '"s1"',
    )
    destination_item = _with_ref(
        _event("event-1", "Original", "20240101T000000Z"),
        "dest:Calendar/event-1.ics",
        '"d1"',
    )
    source = _FakeSource(
        "calendars",
        {"Calendar": [source_item]},
        scopes=[DavScope("Calendar", {"href": "source:Calendar", "displayname": "Calendar"})],
    )
    dest = _FakeDest(
        "calendars",
        {"dest:Calendar": [destination_item]},
        collections=[{"href": "dest:Calendar", "displayname": "Calendar"}],
    )
    reconciler, source, dest, _progress, _logs = _reconciler(
        store, job_id, "calendars", source.items_by_scope, dest.destination, source=source, dest=dest
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Calendar"][0] = _with_ref(
        _event("event-1", "Source edit", "20240201T000000Z"),
        source_item.ref,
        source_item.etag,
    )
    dest.destination["dest:Calendar"][0] = _with_ref(
        _event("event-1", "Destination edit", "20240101T000000Z", "20240301T000000Z"),
        destination_item.ref,
        destination_item.etag,
    )

    reconciler.reconcile()

    assert b"SUMMARY:Destination edit" in source.items_by_scope["Calendar"][0].data
    assert b"SUMMARY:Destination edit" in dest.destination["dest:Calendar"][0].data
    assert len(source.update_calls) == 1
    assert not dest.update_calls


def test_null_hash_pair_adopts_current_values_without_a_push(tmp_path):
    store = db.Store(tmp_path / "null-hash.db")
    job_id = store.create_job("DAV", {}, None)
    source_item = _with_ref(_contact("person-1", "Current"), "source:Default/person-1.vcf", '"s1"')
    destination_item = _with_ref(_contact("person-1", "Current"), "dest:Default/person-1.vcf", '"d1"')
    store.dav_pairs_put_many(
        job_id,
        "contacts",
        [{"key": "Default|person-1", "state": "synced", "src_hash": None, "dst_hash": None}],
    )
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [source_item]},
        {"dest:Default": [destination_item]},
    )

    reconciler.reconcile()

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    assert pair["src_hash"] is not None and pair["dst_hash"] is not None
    assert source.update_calls == dest.update_calls == []


def test_both_sides_present_without_a_pair_are_adopted_without_a_write(tmp_path):
    store = db.Store(tmp_path / "adopt-existing.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "Existing"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "Existing"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )

    reconciler.reconcile()

    assert store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]["state"] == "synced"
    assert source.update_calls == dest.update_calls == []


def test_dav_conditional_update_failure_leaves_pair_unchanged(tmp_path):
    store = db.Store(tmp_path / "stale-update.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "Original"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "Original"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, _progress, logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )
    reconciler.reconcile(baseline=True)
    old_pair = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    source.items_by_scope["Default"][0] = _with_ref(_contact("person-1", "Changed"), src.ref, src.etag)
    dest.update_results.append(False)

    reconciler.reconcile()

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    assert pair["src_hash"] == old_pair["src_hash"]
    assert pair["dst_hash"] == old_pair["dst_hash"]
    assert any("changed during the run; will retry" in message for _level, message in logs)


def test_skipped_copy_back_does_not_write_ledger_and_retries(tmp_path):
    store = db.Store(tmp_path / "copyback-skipped.db")
    job_id = store.create_job("DAV", {}, None)
    extra = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d1"')
    reconciler, source, dest, progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": []}, {"dest:Default": [extra]}
    )
    reconciler.retry = _skip_retry_call(source, 3)

    reconciler.reconcile()

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]
    assert pair["state"] == "copying_back"
    assert store.ledger_get_many(job_id, "contacts", ["Default|dest-only"]) == {}
    assert source.put_calls == []
    assert progress.values.get("copied_back", 0) == 0
    assert dest.delete_calls == []

    reconciler.retry = lambda operation, _connection: operation()
    reconciler.reconcile()

    assert store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]["state"] == "synced"
    assert (
        store.ledger_get_many(job_id, "contacts", ["Default|dest-only"])["Default|dest-only"]["status"]
        == db.SKIPPED
    )
    assert len(source.put_calls) == 1
    assert progress.values["copied_back"] == 1
    assert dest.delete_calls == []


def test_skipped_update_keeps_pair_and_ledger_unchanged_then_retries(tmp_path):
    store = db.Store(tmp_path / "update-skipped.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "Original"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "Original"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, _source, dest, _progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )
    reconciler.reconcile(baseline=True)
    key = "Default|person-1"
    old_pair = store.dav_pairs_get_all(job_id, "contacts")[key]
    store.ledger_mark(job_id, "contacts", key, db.DONE, match_key="person-1", info="original")
    old_ledger = store.ledger_get_many(job_id, "contacts", [key])[key]
    reconciler.source.items_by_scope["Default"][0] = _with_ref(
        _contact("person-1", "Changed"), src.ref, src.etag
    )
    reconciler.retry = _skip_retry_call(dest, 2)

    reconciler.reconcile()

    assert store.dav_pairs_get_all(job_id, "contacts")[key] == old_pair
    assert store.ledger_get_many(job_id, "contacts", [key])[key]["info"] == old_ledger["info"]
    assert dest.update_calls == []

    reconciler.retry = lambda operation, _connection: operation()
    reconciler.reconcile()

    assert len(dest.update_calls) == 1
    assert store.dav_pairs_get_all(job_id, "contacts")[key]["src_hash"] != old_pair["src_hash"]
    assert store.ledger_get_many(job_id, "contacts", [key])[key]["info"] != old_ledger["info"]


def test_unconfirmed_delete_keeps_archive_and_pair_then_retries(tmp_path):
    store = db.Store(tmp_path / "delete-skipped.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    source_anchor = _with_ref(_contact("anchor", "Anchor"), "source:Default/anchor.vcf", '"s2"')
    dest_anchor = _with_ref(_contact("anchor", "Anchor"), "dest:Default/anchor.vcf", '"d2"')
    reconciler, source, dest, progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [src, source_anchor]},
        {"dest:Default": [dst, dest_anchor]},
    )
    reconciler.reconcile(baseline=True)
    key = "Default|person-1"
    store.ledger_mark(job_id, "contacts", key, db.DONE, match_key="person-1", info="original")
    source.items_by_scope["Default"] = [source_anchor]
    reconciler.retry = _skip_retry_call(dest, 2)

    reconciler.reconcile()

    assert key in store.dav_pairs_get_all(job_id, "contacts")
    [uncertain_archive] = store.dav_deleted_list(job_id, "contacts")
    assert uncertain_archive["data"] == dst.data
    assert store.ledger_get_many(job_id, "contacts", [key])[key]["info"] == "original"
    assert dest.delete_calls == []
    assert progress.values.get("trashed", 0) == 0

    reconciler.retry = lambda operation, _connection: operation()
    reconciler.reconcile()

    assert key not in store.dav_pairs_get_all(job_id, "contacts")
    assert len(store.dav_deleted_list(job_id, "contacts")) == 2
    assert len(dest.delete_calls) == 1
    assert progress.values["trashed"] == 1


def test_destination_only_item_copies_back_and_writes_skipped_ledger_row(tmp_path):
    store = db.Store(tmp_path / "copyback.db")
    job_id = store.create_job("DAV", {}, None)
    extra = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d1"')
    reconciler, source, dest, progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": []}, {"dest:Default": [extra]}
    )

    reconciler.reconcile()

    assert len(source.items_by_scope["Default"]) == 1
    assert source.put_calls[0][1] == "dest-only.vcf"
    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]
    assert pair["state"] == "synced"
    ledger = store.ledger_get_many(job_id, "contacts", ["Default|dest-only"])["Default|dest-only"]
    assert ledger["status"] == db.SKIPPED
    assert progress.values["copied_back"] == 1
    assert not dest.delete_calls


def test_copying_back_retries_after_failure_and_resolves_when_both_sides_exist(tmp_path):
    store = db.Store(tmp_path / "copyback-retry.db")
    job_id = store.create_job("DAV", {}, None)
    extra = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d1"')
    source = _FakeSource("contacts", {"Default": []}, put_results=[PermanentError("write failed")])
    dest = _FakeDest("contacts", {"dest:Default": [extra]})
    reconciler, source, dest, _progress, _logs = _reconciler(
        store, job_id, "contacts", source.items_by_scope, dest.destination, source=source, dest=dest
    )

    reconciler.reconcile()
    assert store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]["state"] == "copying_back"
    assert not dest.delete_calls

    reconciler.reconcile()

    pair = store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]
    assert pair["state"] == "synced"
    assert len(source.put_calls) == 2
    assert not dest.delete_calls


def test_copying_back_pair_with_both_objects_present_resolves_without_another_put(tmp_path):
    store = db.Store(tmp_path / "copyback-present.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "Present"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "Present"), "dest:Default/person-1.vcf", '"d1"')
    store.dav_pairs_put_many(
        job_id,
        "contacts",
        [{"key": "Default|person-1", "state": "copying_back", "src_hash": None, "dst_hash": "old"}],
    )
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [src]},
        {"dest:Default": [dst]},
    )

    reconciler.reconcile()

    assert store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]["state"] == "synced"
    assert source.put_calls == dest.put_calls == []
    assert not dest.delete_calls


def test_dav_deletions_save_content_then_delete_in_the_opposite_side(tmp_path):
    store = db.Store(tmp_path / "deletions-both-sides.db")
    job_id = store.create_job("DAV", {}, None)
    src1 = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    src2 = _with_ref(_contact("person-2", "Two"), "source:Default/person-2.vcf", '"s2"')
    dst1 = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    dst2 = _with_ref(_contact("person-2", "Two"), "dest:Default/person-2.vcf", '"d2"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [src1, src2]},
        {"dest:Default": [dst1, dst2]},
    )
    reconciler.reconcile(baseline=True)

    source.items_by_scope["Default"] = [src2]
    reconciler.reconcile()
    [deleted_to_dest] = store.dav_deleted_list(job_id, "contacts")
    assert deleted_to_dest["side"] == "destination"
    assert deleted_to_dest["data"] == dst1.data
    assert len(dest.destination["dest:Default"]) == 1
    assert "Default|person-1" not in store.dav_pairs_get_all(job_id, "contacts")

    store.dav_pairs_put_many(
        job_id,
        "contacts",
        [
            {
                "key": "Default|person-1",
                "state": "synced",
                "src_hash": content_hash(src1.data),
                "dst_hash": content_hash(src1.data),
            }
        ],
    )
    source.items_by_scope["Default"] = [src1, src2]
    dest.destination["dest:Default"] = [dst2]
    reconciler.reconcile()

    deleted = store.dav_deleted_list(job_id, "contacts")
    assert len(deleted) == 2
    assert deleted[1]["side"] == "source"
    assert deleted[1]["data"] == src1.data
    assert {item.uid for item in source.items_by_scope["Default"]} == {"person-2"}


def test_stale_dav_delete_keeps_pair_and_reports_retry(tmp_path):
    store = db.Store(tmp_path / "stale-delete.db")
    job_id = store.create_job("DAV", {}, None)
    src1 = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    src2 = _with_ref(_contact("person-2", "Two"), "source:Default/person-2.vcf", '"s2"')
    dst1 = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    dst2 = _with_ref(_contact("person-2", "Two"), "dest:Default/person-2.vcf", '"d2"')
    reconciler, source, dest, _progress, logs = _reconciler(
        store, job_id, "contacts", {"Default": [src1, src2]}, {"dest:Default": [dst1, dst2]}
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Default"] = [src2]
    dest.delete_results.append(False)

    reconciler.reconcile()

    assert "Default|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert any("changed during the run; will retry" in message for _level, message in logs)
    assert len(store.dav_deleted_list(job_id, "contacts")) == 1


def test_empty_or_missing_paired_scope_suppresses_deletions_and_retains_pairs(tmp_path):
    store = db.Store(tmp_path / "empty-guard.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, _progress, logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Default"] = []

    reconciler.reconcile()

    assert "Default|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert dest.delete_calls == []
    assert any("deletions were suppressed" in message for _level, message in logs)

    source.scopes_list = []
    reconciler.reconcile()

    assert "Default|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert dest.delete_calls == []


def test_failed_other_contact_scope_snapshot_suppresses_deletion_after_move(tmp_path):
    store = db.Store(tmp_path / "failed-move-snapshot.db")
    job_id = store.create_job("DAV", {}, None)
    moved_source = _with_ref(_contact("person-1", "Moved"), "source:A/person-1.vcf", '"s1"')
    anchor_source = _with_ref(_contact("anchor", "Anchor"), "source:A/anchor.vcf", '"s2"')
    moved_dest = _with_ref(_contact("person-1", "Moved"), "dest:Default/person-1.vcf", '"d1"')
    anchor_dest = _with_ref(_contact("anchor", "Anchor"), "dest:Default/anchor.vcf", '"d2"')
    scopes = [
        DavScope("A", {"href": "source:A", "displayname": "A"}),
        DavScope("B", {"href": "source:B", "displayname": "B"}),
    ]
    reconciler, source, dest, _progress, logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"A": [moved_source, anchor_source], "B": []},
        {"dest:Default": [moved_dest, anchor_dest]},
        scopes=scopes,
    )

    reconciler.reconcile(baseline=True)
    source.items_by_scope["A"] = [anchor_source]
    source.items_by_scope["B"] = [moved_source]
    source.strict_failures.add("B")

    reconciler.reconcile()

    assert "A|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert dest.delete_calls == []
    assert store.dav_deleted_list(job_id, "contacts") == []
    assert any("deletions were suppressed" in message for _level, message in logs)


def test_deletion_percent_limit_pauses_until_kind_scoped_approval(tmp_path):
    store = db.Store(tmp_path / "percent-limit.db")
    job_id = store.create_job("DAV", {}, None)
    source_items = [
        _with_ref(
            _contact(f"person-{index}", f"Person {index}"), f"source:Default/{index}.vcf", f'"s{index}"'
        )
        for index in range(20)
    ]
    destination_items = [
        _with_ref(_contact(f"person-{index}", f"Person {index}"), f"dest:Default/{index}.vcf", f'"d{index}"')
        for index in range(20)
    ]
    reconciler, source, dest, _progress, logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": source_items},
        {"dest:Default": destination_items},
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Default"] = source_items[3:]

    assert reconciler.reconcile() is True
    pending = store.get_pending_deletions(job_id, "contacts")
    assert {item["key"] for item in pending} == {
        "Default|person-0",
        "Default|person-1",
        "Default|person-2",
    }
    assert len(dest.destination["dest:Default"]) == 20
    assert store.approve_pending_deletions(job_id)

    assert reconciler.reconcile() is False

    assert len(dest.destination["dest:Default"]) == 17
    assert len(store.dav_deleted_list(job_id, "contacts")) == 3
    assert store.get_pending_deletions(job_id, "contacts") == []
    assert store.get_approved(job_id, "contacts") == set()
    assert any("review and apply" in message for _level, message in logs)


def test_deletion_approval_does_not_carry_over_to_the_other_side(tmp_path):
    store = db.Store(tmp_path / "approval-side.db")
    job_id = store.create_job("DAV", {}, None)
    src1 = _with_ref(_contact("person-1", "One"), "source:Default/1.vcf", '"s1"')
    src2 = _with_ref(_contact("person-2", "Two"), "source:Default/2.vcf", '"s2"')
    dst1 = _with_ref(_contact("person-1", "One"), "dest:Default/1.vcf", '"d1"')
    dst2 = _with_ref(_contact("person-2", "Two"), "dest:Default/2.vcf", '"d2"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [src1, src2]},
        {"dest:Default": [dst1, dst2]},
        options=Options(max_deletions=0, max_deletion_percent=100),
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Default"] = [src2]

    assert reconciler.reconcile() is True
    assert store.get_pending_deletions(job_id, "contacts")[0]["side"] == "destination"
    store.approve_pending_deletions(job_id)

    source.items_by_scope["Default"] = [src1, src2]
    dest.destination["dest:Default"] = [dst2]

    assert reconciler.reconcile() is True
    assert store.get_pending_deletions(job_id, "contacts") == [
        {
            "key": "Default|person-1",
            "side": "source",
            "folder": "Default",
            "subject": "One",
            "kind": "contacts",
        }
    ]
    assert "Default|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert source.items_by_scope["Default"] == [src1, src2]
    assert dest.destination["dest:Default"] == [dst2]


def test_max_deletion_count_pauses_even_for_small_collections(tmp_path):
    store = db.Store(tmp_path / "count-limit.db")
    job_id = store.create_job("DAV", {}, None)
    src1 = _with_ref(_contact("person-1", "One"), "source:Default/1.vcf", '"s1"')
    src2 = _with_ref(_contact("person-2", "Two"), "source:Default/2.vcf", '"s2"')
    dst1 = _with_ref(_contact("person-1", "One"), "dest:Default/1.vcf", '"d1"')
    dst2 = _with_ref(_contact("person-2", "Two"), "dest:Default/2.vcf", '"d2"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [src1, src2]},
        {"dest:Default": [dst1, dst2]},
        options=Options(max_deletions=0, max_deletion_percent=100),
    )
    reconciler.reconcile(baseline=True)
    source.items_by_scope["Default"] = [src2]

    assert reconciler.reconcile() is True
    assert store.get_pending_deletions(job_id, "contacts")[0]["key"] == "Default|person-1"
    assert len(dest.destination["dest:Default"]) == 2


def test_ambiguous_items_are_skipped_and_unmapped_collections_are_left_alone(tmp_path):
    store = db.Store(tmp_path / "ambiguous.db")
    job_id = store.create_job("DAV", {}, None)
    duplicate_a = _with_ref(_contact("duplicate", "One"), "source:Default/a.vcf", '"s1"')
    duplicate_b = _with_ref(_contact("duplicate", "Two"), "source:Default/b.vcf", '"s2"')
    dest_item = _with_ref(_contact("duplicate", "One"), "dest:Default/duplicate.vcf", '"d1"')
    collections = [
        {"href": "dest:Default", "displayname": "Default"},
        {"href": "dest:Other", "displayname": "Other"},
    ]
    other_item = _with_ref(_contact("unmapped", "Other"), "dest:Other/unmapped.vcf", '"d2"')
    reconciler, source, dest, _progress, logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Default": [duplicate_a, duplicate_b]},
        {"dest:Default": [dest_item], "dest:Other": [other_item]},
        dest=_FakeDest(
            "contacts",
            {"dest:Default": [dest_item], "dest:Other": [other_item]},
            collections=collections,
        ),
    )

    reconciler.reconcile(baseline=True)

    assert store.dav_pairs_get_all(job_id, "contacts") == {}
    assert source.put_calls == dest.put_calls == []
    assert any("ambiguous DAV item" in message for _level, message in logs)
    assert any("no source mapping" in message for _level, message in logs)
    assert dest.destination["dest:Other"] == [other_item]


def test_ambiguous_destination_pair_is_never_deleted_or_updated(tmp_path):
    store = db.Store(tmp_path / "ambiguous-pair.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "One"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, _progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )
    reconciler.reconcile(baseline=True)
    duplicate = _with_ref(_contact("person-1", "Duplicate"), "dest:Default/duplicate.vcf", '"d2"')
    dest.destination["dest:Default"].append(duplicate)

    reconciler.reconcile()

    assert "Default|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert dest.delete_calls == []
    assert dest.update_calls == []
    assert source.update_calls == []


def test_destination_deletion_is_skipped_when_another_source_scope_maps_to_uid(tmp_path):
    store = db.Store(tmp_path / "other-source-record.db")
    job_id = store.create_job("DAV", {}, None)
    scopes = [
        DavScope("Work", {"href": "source:Work", "displayname": "Work"}),
        DavScope("Default", {"href": "source:Default", "displayname": "Default"}),
    ]
    work_item = _with_ref(_contact("person-1", "One"), "source:Work/person-1.vcf", '"sw1"')
    work_other = _with_ref(_contact("other", "Other"), "source:Work/other.vcf", '"sw2"')
    destination_item = _with_ref(_contact("person-1", "One"), "dest:Default/person-1.vcf", '"d1"')
    destination_other = _with_ref(_contact("other", "Other"), "dest:Default/other.vcf", '"d2"')
    reconciler, _source, dest, _progress, _logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Work": [work_item, work_other], "Default": []},
        {"dest:Default": [destination_item, destination_other]},
        scopes=scopes,
    )
    reconciler.reconcile(baseline=True)
    reconciler.source.items_by_scope["Work"] = [work_other]
    replacement = _with_ref(_contact("person-1", "One elsewhere"), "source:Default/person-1.vcf", '"sd1"')
    reconciler.source.items_by_scope["Default"] = [replacement]

    reconciler.reconcile()

    assert "Work|person-1" in store.dav_pairs_get_all(job_id, "contacts")
    assert dest.delete_calls == []
    assert any(item.uid == "person-1" for item in dest.destination["dest:Default"])


def test_contacts_copyback_reverse_mapping_prefers_default_scope(tmp_path):
    store = db.Store(tmp_path / "reverse-map.db")
    job_id = store.create_job("DAV", {}, None)
    scopes = [
        DavScope("Work", {"href": "source:Work", "displayname": "Work"}),
        DavScope("default", {"href": "source:default", "displayname": "default"}),
    ]
    extra = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d1"')
    reconciler, source, _dest, _progress, _logs = _reconciler(
        store, job_id, "contacts", {"Work": [], "default": []}, {"dest:Default": [extra]}, scopes=scopes
    )

    reconciler.reconcile()

    assert source.items_by_scope["default"][0].uid == "dest-only"
    assert not source.items_by_scope["Work"]
    assert "default|dest-only" in store.dav_pairs_get_all(job_id, "contacts")


def test_copyback_conflicts_remain_tracked_with_one_summary_warning(tmp_path):
    store = db.Store(tmp_path / "copyback-conflict.db")
    job_id = store.create_job("DAV", {}, None)
    extra = _with_ref(_contact("dest-only", "Extra"), "dest:Default/dest-only.vcf", '"d1"')
    source = _FakeSource(
        "contacts", {"Default": []}, put_results=[PermanentError("DAV PUT returned HTTP 409")]
    )
    dest = _FakeDest("contacts", {"dest:Default": [extra]})
    reconciler, source, _dest, _progress, logs = _reconciler(
        store, job_id, "contacts", source.items_by_scope, dest.destination, source=source, dest=dest
    )

    reconciler.reconcile()

    assert len(source.put_calls) == 1
    assert store.dav_pairs_get_all(job_id, "contacts")["Default|dest-only"]["state"] == "copying_back"
    assert sum("could not be copied back" in message for _level, message in logs) == 1


def test_multiple_source_scopes_with_same_uid_and_ambiguous_dest_only_are_skipped(tmp_path):
    store = db.Store(tmp_path / "multi-scope-ambiguous.db")
    job_id = store.create_job("DAV", {}, None)
    scopes = [
        DavScope("Work", {"href": "source:Work", "displayname": "Work"}),
        DavScope("Personal", {"href": "source:Personal", "displayname": "Personal"}),
    ]
    work_item = _with_ref(_contact("same", "Work"), "source:Work/same.vcf", '"s1"')
    personal_item = _with_ref(_contact("same", "Personal"), "source:Personal/same.vcf", '"s2"')
    duplicate_dest = _with_ref(_contact("same", "Destination"), "dest:Default/same.vcf", '"d1"')
    unrelated = _with_ref(
        _contact("ambiguous-only", "Ambiguous"),
        "dest:Default/ambiguous-only.vcf",
        '"d2"',
    )
    unrelated.ambiguous = True
    reconciler, source, dest, _progress, logs = _reconciler(
        store,
        job_id,
        "contacts",
        {"Work": [work_item], "Personal": [personal_item]},
        {"dest:Default": [duplicate_dest, unrelated]},
        scopes=scopes,
    )

    reconciler.reconcile(baseline=True)
    reconciler.reconcile()

    assert store.dav_pairs_get_all(job_id, "contacts") == {}
    assert source.put_calls == dest.put_calls == []
    assert any("ambiguous DAV item" in message for _level, message in logs)


def test_plan_only_counts_reconciliation_without_writing_state_or_objects(tmp_path):
    store = db.Store(tmp_path / "plan-only.db")
    job_id = store.create_job("DAV", {}, None)
    src = _with_ref(_contact("person-1", "Original"), "source:Default/person-1.vcf", '"s1"')
    dst = _with_ref(_contact("person-1", "Original"), "dest:Default/person-1.vcf", '"d1"')
    reconciler, source, dest, progress, _logs = _reconciler(
        store, job_id, "contacts", {"Default": [src]}, {"dest:Default": [dst]}
    )
    reconciler.reconcile(baseline=True)
    before = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    source.items_by_scope["Default"][0] = _with_ref(_contact("person-1", "Changed"), src.ref, src.etag)

    reconciler.reconcile(plan_only=True)

    after = store.dav_pairs_get_all(job_id, "contacts")["Default|person-1"]
    assert after["src_hash"] == before["src_hash"]
    assert dest.destination["dest:Default"][0].data == dst.data
    assert dest.update_calls == []
    assert progress.values["updated"] == 1
    assert store.get_pending_deletions(job_id, "contacts") == []
