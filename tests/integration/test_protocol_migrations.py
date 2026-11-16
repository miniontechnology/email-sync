from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from imapclient import IMAPClient

from emailsync import db
from emailsync.connectors.dav import DavClient
from emailsync.connectors.imap import ssl_context
from emailsync.connectors.jmap import MAIL, JmapClient, keywords_to_flags
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.pipeline import DavDestination, DavMigration
from emailsync.dav.vobjects import ensure_vcard_uid
from emailsync.jobs.control import RunControl
from emailsync.mail.dest import make_dest
from emailsync.mail.fingerprint import key_from_jmap, summary_from_headers
from emailsync.mail.index import DestIndex
from emailsync.mail.pipeline import MailMigration
from emailsync.mail.state import StateChange
from emailsync.options import Endpoint, JobConfig, Options

pytestmark = pytest.mark.integration

ROOT = Path(__file__).parent
STALWART_USER = "alice@example.org"
STALWART_PASSWORD = "Vx7-quartz-Falcon-river"


def _imap(endpoint):
    conn = IMAPClient(
        endpoint.host,
        port=endpoint.effective_port(),
        ssl=endpoint.security == "ssl",
        ssl_context=ssl_context(endpoint.insecure_tls),
    )
    conn.login(endpoint.username, endpoint.password)
    return conn


def _seed_greenmail(messages):
    endpoint = Endpoint(
        "imap", host="127.0.0.1", port=3143, security="none", username="alice", password="sourcepass"
    )
    conn = _imap(endpoint)
    try:
        for _flags, _delimiter, folder in conn.list_folders():
            name = folder.decode() if isinstance(folder, bytes) else str(folder)
            try:
                conn.select_folder(name)
                uids = conn.search(["ALL"])
                if uids:
                    conn.delete_messages(uids)
                    conn.expunge()
            except Exception:
                continue
        for folder, raw in messages:
            if folder != "INBOX":
                try:
                    conn.create_folder(folder)
                except Exception:
                    pass
            conn.append(folder, raw, msg_time=datetime.now(UTC))
    finally:
        conn.logout()
    return endpoint


def _raw(message_id, subject):
    return (
        f"Message-ID: <{message_id}>\r\nDate: Tue, 01 Jan 2019 12:00:00 +0000\r\n"
        "From: sender@example.test\r\nTo: recipient@example.test\r\n"
        f"Subject: {subject}\r\n\r\nBody for {subject}.\r\n"
    ).encode()


def _mail_config(source, destination, workers=2):
    return JobConfig(
        "Integration mail",
        {
            "mail": {"source": source, "dest": destination},
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(workers=workers, verify=True),
    )


def _run_mail(
    tmp_path, source, destination, config=None, control=None, dest_factory=make_dest, store=None, job_id=None
):
    store = store or db.Store(tmp_path / "integration.db")
    job_id = job_id or store.create_job("integration", {}, None)
    migration = MailMigration(
        store,
        job_id,
        config or _mail_config(source, destination),
        control or RunControl(),
        dest_factory=dest_factory,
    )
    result = migration.run()
    return result, store, job_id


def _clear_stalwart():
    conn = _imap(_destination_imap())
    try:
        for _flags, _delimiter, folder in conn.list_folders():
            name = folder.decode() if isinstance(folder, bytes) else str(folder)
            try:
                conn.select_folder(name)
                uids = conn.search(["ALL"])
                if uids:
                    conn.delete_messages(uids)
                    conn.expunge()
            except Exception:
                continue
    finally:
        conn.logout()


def _destination_imap():
    return Endpoint(
        "imap",
        host="127.0.0.1",
        port=10993,
        security="ssl",
        username=STALWART_USER,
        password=STALWART_PASSWORD,
        insecure_tls=True,
    )


def _destination_jmap():
    return Endpoint(
        "jmap",
        url="http://127.0.0.1:18080",
        username=STALWART_USER,
        password=STALWART_PASSWORD,
    )


def _assert_destination_count(folder, count):
    conn = _imap(_destination_imap())
    try:
        conn.select_folder(folder, readonly=True)
        assert len(conn.search(["ALL"])) == count
    finally:
        conn.logout()


def test_greenmail_to_stalwart_imap_is_idempotent(tmp_path):
    _clear_stalwart()
    source = _seed_greenmail([("INBOX", _raw("imap-one", "one")), ("Work", _raw("imap-two", "two"))])
    destination = _destination_imap()
    result, store, job_id = _run_mail(tmp_path, source, destination)
    assert result.status == "completed", result.error
    _assert_destination_count("INBOX", 1)
    _assert_destination_count("Work", 1)
    result, store, job_id = _run_mail(tmp_path, source, destination, store=store, job_id=job_id)
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0


def test_greenmail_to_stalwart_jmap_and_resume_after_pause(tmp_path):
    _clear_stalwart()
    source = _seed_greenmail([("INBOX", _raw(f"jmap-{i}", f"message-{i}")) for i in range(12)])
    destination = Endpoint(
        "jmap", url="http://127.0.0.1:18080", username=STALWART_USER, password=STALWART_PASSWORD
    )
    control = RunControl()
    inner_factory = make_dest

    class PausingDestination:
        def __init__(self, endpoint, options):
            self.inner = inner_factory(endpoint, options)
            self.uploads = 0
            self.index_mode = self.inner.index_mode

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def upload(self, *args):
            result = self.inner.upload(*args)
            self.uploads += 1
            if self.uploads == 3:
                control.stop("pause")
            return result

    result, store, job_id = _run_mail(
        tmp_path,
        source,
        destination,
        control=control,
        dest_factory=lambda endpoint, options: PausingDestination(endpoint, options),
    )
    assert result.status == "paused", result.error
    resumed, _store, _job = _run_mail(tmp_path, source, destination, store=store, job_id=job_id)
    assert resumed.status == "completed"
    _assert_destination_count("INBOX", 12)
    rerun, _store, _job = _run_mail(tmp_path, source, destination, store=store, job_id=job_id)
    assert rerun.status == "completed"
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert store.get_job(job_id) is not None


@pytest.mark.parametrize("destination_kind", ["imap", "jmap"])
def test_repeat_mail_sync_reconciles_source_flags_without_duplicate_messages(tmp_path, destination_kind):
    _clear_stalwart()
    raw_messages = [_raw(f"state-{uuid4()}-{i}", f"state-{i}") for i in range(2)]
    expected_keys = [summary_from_headers(raw)["key"] for raw in raw_messages]
    source = _seed_greenmail([("INBOX", raw) for raw in raw_messages])
    source_conn = _imap(source)
    try:
        source_conn.select_folder("INBOX", readonly=False)
        source_uids = sorted(source_conn.search(["ALL"]))
        source_conn.add_flags([source_uids[1]], ["\\Seen"], silent=True)
    finally:
        source_conn.logout()

    destination = _destination_imap() if destination_kind == "imap" else _destination_jmap()
    config = _mail_config(source, destination)
    result, store, job_id = _run_mail(tmp_path, source, destination, config=config)
    assert result.status == "completed", result.error

    source_conn = _imap(source)
    try:
        source_conn.select_folder("INBOX", readonly=False)
        source_conn.add_flags([source_uids[0]], ["\\Seen", "\\Flagged"], silent=True)
        source_conn.remove_flags([source_uids[1]], ["\\Seen"], silent=True)
    finally:
        source_conn.logout()

    result, _store, _job_id = _run_mail(
        tmp_path, source, destination, config=config, store=store, job_id=job_id
    )
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["updated"] == 2
    assert store.get_tasks(job_id)[0]["copied"] == 0

    if destination_kind == "imap":
        dest_conn = _imap(destination)
        try:
            dest_conn.select_folder("INBOX", readonly=True)
            dest_uids = sorted(dest_conn.search(["ALL"]))
            flags = dest_conn.fetch(dest_uids, [b"FLAGS"])
            assert len(dest_uids) == 2
            assert {flag.lower() for flag in flags[dest_uids[0]][b"FLAGS"]} >= {
                b"\\seen",
                b"\\flagged",
            }
            assert b"\\seen" not in {flag.lower() for flag in flags[dest_uids[1]][b"FLAGS"]}
        finally:
            dest_conn.logout()
    else:
        client = JmapClient(destination)
        try:
            client.connect(MAIL)
            emails = list(client.iter_email_index())
            assert len(emails) == 2
            states = {key_from_jmap(email): keywords_to_flags(email.get("keywords", {})) for email in emails}
            assert states[expected_keys[0]] >= frozenset({"\\seen", "\\flagged"})
            assert "\\seen" not in states[expected_keys[1]]
        finally:
            client.close()


@pytest.mark.parametrize("destination_kind", ["imap", "jmap"])
def test_stalwart_dest_apply_state_moves_only_copy_and_preserves_flags(destination_kind):
    _clear_stalwart()
    endpoint = _destination_imap() if destination_kind == "imap" else _destination_jmap()
    destination = make_dest(endpoint, Options(sync_flags_labels=True))
    raw = _raw(f"move-{destination_kind}-{uuid4()}", "move")
    key = summary_from_headers(raw)["key"]
    try:
        destination.connect()
        inbox, _ = destination.resolve("INBOX", create=False)
        work, _ = destination.resolve("Work", create=True)
        destination.upload(raw, [inbox], ["\\Seen"], datetime.now(UTC), key, DestIndex())
        before = DestIndex()
        if destination_kind == "imap":
            destination.index_folder(inbox, before)
        else:
            destination.index_all(before)
        copies = before.copies(key)
        assert len(copies) == 1
        change = StateChange(key, copies, [], [work], [inbox])
        errors = destination.apply_state([change], retry=lambda operation: operation())
        assert errors == {}

        after = DestIndex()
        if destination_kind == "imap":
            destination.index_folder(inbox, after)
            destination.index_folder(work, after)
        else:
            destination.index_all(after)
        copies = after.copies(key)
        assert len(copies) == 1
        assert copies[0].folders == frozenset({work})
        assert "\\seen" in copies[0].flags
    finally:
        destination.close()


def test_preseeded_destination_only_receives_missing_messages(tmp_path):
    _clear_stalwart()
    messages = [("INBOX", _raw(f"seed-{i}", f"seed-{i}")) for i in range(4)]
    source = _seed_greenmail(messages)
    destination = _destination_imap()
    conn = _imap(destination)
    try:
        for _folder, raw in messages[:2]:
            conn.append("INBOX", raw, msg_time=datetime.now(UTC))
    finally:
        conn.logout()
    result, store, job_id = _run_mail(tmp_path, source, destination)
    assert result.status == "completed", result.error
    _assert_destination_count("INBOX", 4)
    assert store.get_tasks(job_id)[0]["copied"] == 2


def _seed_radicale(kind, collection, filename, data):
    root = f"http://127.0.0.1:15232/alice/{collection}/"
    auth = ("alice", "radicalepass")
    method = "MKCALENDAR" if kind == "calendars" else "MKCOL"
    root_tag = "k:mkcalendar" if kind == "calendars" else "d:mkcol"
    resource = (
        "" if kind == "calendars" else "<d:resourcetype><d:collection/><c:addressbook/></d:resourcetype>"
    )
    xml = (
        f'<?xml version="1.0"?><{root_tag} xmlns:d="DAV:" '
        'xmlns:c="urn:ietf:params:xml:ns:carddav" '
        'xmlns:k="urn:ietf:params:xml:ns:caldav"><d:set><d:prop>'
        f"{resource}<d:displayname>{collection}</d:displayname>"
        f"</d:prop></d:set></{root_tag}>"
    )
    response = httpx.request(method, root, auth=auth, content=xml)
    assert response.status_code in (200, 201, 204, 405, 409)
    response = httpx.put(root + filename, auth=auth, content=data)
    assert response.status_code in (200, 201, 204, 412)
    return root


@pytest.mark.parametrize(
    ("kind", "collection", "filename", "data"),
    [
        (
            "contacts",
            "contacts",
            "person.vcf",
            b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:person-1\r\nFN:Person\r\nEND:VCARD\r\n",
        ),
        (
            "calendars",
            "calendar",
            "event.ics",
            b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\n"
            b"BEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Event\r\nDTSTART:20250101T120000Z\r\n"
            b"END:VEVENT\r\nEND:VCALENDAR\r\n",
        ),
    ],
)
def test_radicale_dav_and_file_sources_to_stalwart(tmp_path, kind, collection, filename, data):
    if kind == "contacts":
        _cleanup_dav_uid_prefix(
            Endpoint(
                "carddav",
                url="http://127.0.0.1:15232/alice/contacts/",
                username="alice",
                password="radicalepass",
            ),
            "contacts",
            "changed-",
        )
    source_url = _seed_radicale(kind, collection, filename, data)
    endpoint_type = "carddav" if kind == "contacts" else "caldav"
    source = Endpoint(endpoint_type, url=source_url, username="alice", password="radicalepass")
    destination = Endpoint(
        endpoint_type, url="http://127.0.0.1:18080", username=STALWART_USER, password=STALWART_PASSWORD
    )
    config = JobConfig(
        "DAV integration",
        {
            kind: {"source": source, "dest": destination},
            **{
                other: {"source": None, "dest": None}
                for other in ("mail", "contacts", "calendars")
                if other != kind
            },
        },
        Options(workers=1, verify=False),
    )
    store = db.Store(tmp_path / f"{kind}.db")
    job_id = store.create_job(kind, {}, None)
    result = DavMigration(store, job_id, kind, config, RunControl()).run()
    assert result.status == "completed", result.error
    result = DavMigration(store, job_id, kind, config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["already"] == 1

    file_source = tmp_path / filename
    file_source.write_bytes(data)
    file_config = JobConfig(
        f"{kind} file integration",
        {
            kind: {"source": Endpoint("file", files=[str(file_source)]), "dest": destination},
            **{
                other: {"source": None, "dest": None}
                for other in ("mail", "contacts", "calendars")
                if other != kind
            },
        },
        Options(workers=1, verify=False),
    )
    file_job = store.create_job(f"{kind} file", {}, None)
    result = DavMigration(store, file_job, kind, file_config, RunControl()).run()
    assert result.status == "completed", result.error


def _file_dav_config(kind, file_path, destination):
    return JobConfig(
        f"{kind} JMAP import",
        {
            kind: {"source": Endpoint("file", files=[str(file_path)]), "dest": destination},
            **{
                other: {"source": None, "dest": None}
                for other in ("mail", "contacts", "calendars")
                if other != kind
            },
        },
        Options(workers=1, verify=False),
    )


def _jmap_destination():
    return Endpoint("jmap", url="http://127.0.0.1:18080", username=STALWART_USER, password=STALWART_PASSWORD)


def _query_jmap_items(kind, displayname):
    destination = JmapDavDestination(_jmap_destination(), kind).connect()
    try:
        if displayname.casefold() == "default":
            collection = next(
                (item for item in destination.collections if item["displayname"].casefold() == "default"),
                destination.collections[0],
            )
        else:
            collection = next(item for item in destination.collections if item["displayname"] == displayname)
        return destination.items(collection)
    finally:
        destination.close()


def _jmap_object(kind, displayname, uid):
    destination = JmapDavDestination(_jmap_destination(), kind).connect()
    try:
        collection = next(
            (
                item
                for item in destination.collections
                if item["displayname"].casefold() == displayname.casefold()
            ),
            destination.collections[0],
        )
        item = next(item for item in destination.items(collection) if item.uid == uid)
        response = destination.client.call(
            [
                [
                    f"{destination.object_name}/get",
                    {"accountId": destination.client.account_id, "ids": [item.ref]},
                    "g",
                ]
            ],
            using=destination._using(),
        )
        return item, response[0][1]["list"][0], collection
    finally:
        destination.close()


def _dav_items(endpoint, kind, displayname=None):
    destination = DavDestination(endpoint, kind).connect()
    try:
        collection = next(
            (
                item
                for item in destination.collections
                if displayname and item["displayname"].casefold() == displayname.casefold()
            ),
            destination.collections[0],
        )
        return destination.items(collection), collection
    finally:
        destination.close()


def _cleanup_dav_uid_prefix(endpoint, kind, prefix):
    destination = DavDestination(endpoint, kind).connect()
    try:
        for collection in destination.collections:
            for item in destination.items(collection):
                if item.uid.startswith(prefix):
                    destination.client._request("DELETE", item.ref)
    finally:
        destination.close()


def _cleanup_jmap_uid_prefix(kind, displayname, prefix):
    destination = JmapDavDestination(_jmap_destination(), kind).connect()
    try:
        collection = next(
            (
                item
                for item in destination.collections
                if item["displayname"].casefold() == displayname.casefold()
            ),
            destination.collections[0],
        )
        refs = [item.ref for item in destination.items(collection) if item.uid.startswith(prefix)]
        if refs:
            args = {"accountId": destination.client.account_id, "destroy": refs}
            if kind == "calendars":
                args["sendSchedulingMessages"] = False
            destination.client.call(
                [[f"{destination.object_name}/set", args, "d"]],
                using=destination._using(),
            )
    finally:
        destination.close()


def _destroy_jmap_object(kind, displayname, uid, delete_collection=False):
    destination = JmapDavDestination(_jmap_destination(), kind).connect()
    try:
        collection = next(
            (
                item
                for item in destination.collections
                if item["displayname"].casefold() == displayname.casefold()
            ),
            destination.collections[0],
        )
        item = next((item for item in destination.items(collection) if item.uid == uid), None)
        if item:
            destination.client.call(
                [
                    [
                        f"{destination.object_name}/set",
                        {
                            "accountId": destination.client.account_id,
                            "destroy": [item.ref],
                            **({"sendSchedulingMessages": False} if kind == "calendars" else {}),
                        },
                        "d",
                    ]
                ],
                using=destination._using(),
            )
        if delete_collection:
            destination.client.call(
                [
                    [
                        f"{destination.collection_name}/set",
                        {"accountId": destination.client.account_id, "destroy": [collection["id"]]},
                        "c",
                    ]
                ],
                using=destination._using(),
            )
    finally:
        destination.close()


def _dav_config(kind, source, destination):
    return JobConfig(
        f"{kind} update integration",
        {
            kind: {"source": source, "dest": destination},
            **{
                other: {"source": None, "dest": None}
                for other in ("mail", "contacts", "calendars")
                if other != kind
            },
        },
        Options(workers=1, verify=False),
    )


def _contact_vcard(uid, name, phone=True):
    lines = ["BEGIN:VCARD", "VERSION:3.0", f"UID:{uid}", f"FN:{name}", f"EMAIL:{uid}@example.org"]
    if phone:
        lines.append("TEL;TYPE=WORK:+1-555-0100")
    lines.append("END:VCARD")
    return ("\r\n".join(lines) + "\r\n").encode()


def _recurring_calendar(uid, changed=False):
    summary = "Weekly planning changed" if changed else "Weekly planning"
    exdate = "EXDATE:20300121T090000Z\r\n" if changed else ""
    return (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "PRODID:-//Email Sync Integration Tests//EN\r\n"
        "BEGIN:VEVENT\r\n"
        f"UID:{uid}\r\n"
        "DTSTAMP:20261116T120000Z\r\n"
        "DTSTART:20300107T090000Z\r\n"
        "DTEND:20300107T100000Z\r\n"
        "RRULE:FREQ=WEEKLY;COUNT=4\r\n"
        f"SUMMARY:{summary}\r\n"
        f"{exdate}"
        "END:VEVENT\r\n"
        "BEGIN:VEVENT\r\n"
        f"UID:{uid}\r\n"
        "RECURRENCE-ID:20300114T090000Z\r\n"
        "DTSTAMP:20261116T120000Z\r\n"
        "DTSTART:20300114T103000Z\r\n"
        "DTEND:20300114T113000Z\r\n"
        "SUMMARY:Rescheduled planning\r\n"
        "END:VEVENT\r\n"
        "END:VCALENDAR\r\n"
    ).encode()


def test_file_contacts_to_stalwart_jmap_is_idempotent(tmp_path):
    file_path = tmp_path / "contacts-jmap.vcf"
    suffix = uuid4().hex
    second_card = (
        f"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Two Person {suffix}\r\n"
        f"EMAIL:two-{suffix}@example.org\r\nEND:VCARD\r\n"
    )
    second_uid, _card = ensure_vcard_uid(second_card)
    file_path.write_bytes(
        (
            f"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:jmap-contact-{suffix}\r\nFN:One Person\r\n"
            f"EMAIL:one-{suffix}@example.org\r\nEND:VCARD\r\n" + second_card
        ).encode()
    )
    config = _file_dav_config("contacts", file_path, _jmap_destination())
    store = db.Store(tmp_path / "contacts-jmap.db")
    job_id = store.create_job("JMAP contacts", {}, None)

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 2
    items = _query_jmap_items("contacts", "Default")
    expected_uids = {f"jmap-contact-{suffix}", second_uid}
    assert expected_uids <= {item.uid for item in items}

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert expected_uids <= {item.uid for item in _query_jmap_items("contacts", "Default")}


def test_file_calendar_with_recurrence_to_stalwart_jmap_is_idempotent(tmp_path):
    file_path = tmp_path / "team-calendar-jmap.ics"
    event_uid = f"jmap-weekly-{uuid4().hex}".encode()
    file_path.write_bytes(
        b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Email Sync Tests//EN\r\n"
        b"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n"
        b"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0400\r\n"
        b"TZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n"
        b"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nTZOFFSETFROM:-0500\r\n"
        b"TZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n"
        b"BEGIN:VEVENT\r\nUID:" + event_uid + b"\r\nDTSTAMP:20240101T000000Z\r\n"
        b"DTSTART;TZID=America/New_York:20250106T090000\r\n"
        b"DTEND;TZID=America/New_York:20250106T100000\r\n"
        b"RRULE:FREQ=WEEKLY;COUNT=4\r\nSUMMARY:Weekly planning\r\nEND:VEVENT\r\n"
        b"BEGIN:VEVENT\r\nUID:" + event_uid + b"\r\n"
        b"RECURRENCE-ID;TZID=America/New_York:20250113T090000\r\n"
        b"DTSTAMP:20240101T000000Z\r\n"
        b"DTSTART;TZID=America/New_York:20250113T103000\r\n"
        b"DTEND;TZID=America/New_York:20250113T113000\r\n"
        b"SUMMARY:Rescheduled planning\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
    )
    config = _file_dav_config("calendars", file_path, _jmap_destination())
    store = db.Store(tmp_path / "calendar-jmap.db")
    job_id = store.create_job("JMAP calendar", {}, None)

    result = DavMigration(store, job_id, "calendars", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 1
    items = _query_jmap_items("calendars", file_path.stem)
    assert [item.uid for item in items].count(event_uid.decode()) == 1
    assert next(item for item in items if item.uid == event_uid.decode()).fingerprint == ""

    result = DavMigration(store, job_id, "calendars", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert [item.uid for item in _query_jmap_items("calendars", file_path.stem)].count(
        event_uid.decode()
    ) == 1


def test_carddav_preseeded_contact_is_skipped_by_jmap_destination(tmp_path):
    carddav = DavClient(
        Endpoint(
            "carddav",
            url="http://127.0.0.1:18080",
            username=STALWART_USER,
            password=STALWART_PASSWORD,
        ),
        "carddav",
    )
    uid = f"cross-protocol-contact-{uuid4().hex}"
    contact_data = (
        f"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:{uid}\r\n"
        f"FN:Shared Person\r\nEMAIL:{uid}@example.org\r\nEND:VCARD\r\n"
    ).encode()
    try:
        _home, collections = carddav.discover()
        assert collections
        default_book = collections[0]
        assert carddav.put(default_book, f"{uid}.vcf", contact_data) is True
    finally:
        carddav.close()

    file_path = tmp_path / "cross-protocol.vcf"
    file_path.write_bytes(contact_data)
    config = _file_dav_config("contacts", file_path, _jmap_destination())
    store = db.Store(tmp_path / "cross-protocol.db")
    job_id = store.create_job("Cross-protocol contact", {}, None)

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert store.get_tasks(job_id)[0]["skipped"] == 1


def test_radicale_contacts_update_stalwart_carddav(tmp_path):
    uid = f"changed-carddav-{uuid4().hex}"
    filename = f"{uid}.vcf"
    source_endpoint = Endpoint(
        "carddav",
        url="http://127.0.0.1:15232/alice/contacts/",
        username="alice",
        password="radicalepass",
    )
    destination_endpoint = Endpoint(
        "carddav",
        url="http://127.0.0.1:18080",
        username=STALWART_USER,
        password=STALWART_PASSWORD,
    )
    _cleanup_dav_uid_prefix(source_endpoint, "contacts", "changed-carddav-")
    _cleanup_dav_uid_prefix(destination_endpoint, "contacts", "changed-carddav-")
    source_url = _seed_radicale("contacts", "contacts", filename, _contact_vcard(uid, "Before"))
    source = Endpoint("carddav", url=source_url, username="alice", password="radicalepass")
    destination = Endpoint(
        "carddav",
        url="http://127.0.0.1:18080",
        username=STALWART_USER,
        password=STALWART_PASSWORD,
    )
    config = _dav_config("contacts", source, destination)
    store = db.Store(tmp_path / "update-carddav.db")
    job_id = store.create_job("Update CardDAV contact", {}, None)

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    before = [item for item in _dav_items(destination, "contacts")[0] if item.uid == uid]
    assert len(before) == 1

    changed = _contact_vcard(uid, "After", phone=False)
    response = httpx.put(source_url + filename, auth=("alice", "radicalepass"), content=changed)
    assert response.status_code in (200, 201, 204)
    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    after = [item for item in _dav_items(destination, "contacts")[0] if item.uid == uid]
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert len(after) == len(before) == 1
    assert b"FN:After" in after[0].data
    assert b"TEL:" not in after[0].data
    _cleanup_dav_uid_prefix(source_endpoint, "contacts", "changed-carddav-")
    _cleanup_dav_uid_prefix(destination_endpoint, "contacts", "changed-carddav-")


@pytest.mark.parametrize("source_type", ("file", "radicale"))
def test_changed_contacts_update_stalwart_jmap(tmp_path, source_type):
    uid = f"changed-jmap-{uuid4().hex}"
    filename = f"{uid}.vcf"
    _cleanup_jmap_uid_prefix("contacts", "Default", "changed-jmap-")
    radicale_endpoint = Endpoint(
        "carddav",
        url="http://127.0.0.1:15232/alice/contacts/",
        username="alice",
        password="radicalepass",
    )
    if source_type == "radicale":
        _cleanup_dav_uid_prefix(radicale_endpoint, "contacts", "changed-jmap-")
    original = _contact_vcard(uid, "Before")
    changed = _contact_vcard(uid, "After", phone=False)
    if source_type == "file":
        file_path = tmp_path / filename
        file_path.write_bytes(original)
        source = Endpoint("file", files=[str(file_path)])
    else:
        source_url = _seed_radicale("contacts", "contacts", filename, original)
        source = Endpoint("carddav", url=source_url, username="alice", password="radicalepass")
    config = _file_dav_config("contacts", tmp_path / filename, _jmap_destination())
    config.endpoints["contacts"]["source"] = source
    store = db.Store(tmp_path / f"update-jmap-{source_type}.db")
    job_id = store.create_job("Update JMAP contact", {}, None)

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    before_items = [item for item in _query_jmap_items("contacts", "Default") if item.uid == uid]
    assert len(before_items) == 1

    if source_type == "file":
        file_path.write_bytes(changed)
    else:
        response = httpx.put(source_url + filename, auth=("alice", "radicalepass"), content=changed)
        assert response.status_code in (200, 201, 204)
    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    after_items = [item for item in _query_jmap_items("contacts", "Default") if item.uid == uid]
    _item, contact, _collection = _jmap_object("contacts", "Default", uid)
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert len(after_items) == len(before_items) == 1
    assert contact["name"]["full"] == "After"
    assert not contact.get("phones")
    _destroy_jmap_object("contacts", "Default", uid)
    if source_type == "radicale":
        _cleanup_dav_uid_prefix(radicale_endpoint, "contacts", "changed-jmap-")


@pytest.mark.parametrize("destination_type", ("caldav", "jmap"))
def test_changed_recurring_event_updates_stalwart_destination(tmp_path, destination_type):
    uid = f"changed-event-{uuid4().hex}"
    file_path = tmp_path / f"event-update-{uuid4().hex}.ics"
    file_path.write_bytes(_recurring_calendar(uid))
    if destination_type == "jmap":
        destination = _jmap_destination()
    else:
        destination = Endpoint(
            "caldav",
            url="http://127.0.0.1:18080",
            username=STALWART_USER,
            password=STALWART_PASSWORD,
        )
    config = _file_dav_config("calendars", file_path, destination)
    store = db.Store(tmp_path / f"update-event-{destination_type}.db")
    job_id = store.create_job("Update recurring event", {}, None)

    result = DavMigration(store, job_id, "calendars", config, RunControl()).run()
    assert result.status == "completed", result.error
    if destination_type == "jmap":
        before_items = [item for item in _query_jmap_items("calendars", file_path.stem) if item.uid == uid]
    else:
        before_items = [
            item for item in _dav_items(destination, "calendars", file_path.stem)[0] if item.uid == uid
        ]
    assert len(before_items) == 1

    file_path.write_bytes(_recurring_calendar(uid, changed=True))
    result = DavMigration(store, job_id, "calendars", config, RunControl()).run()
    assert result.status == "completed", result.error
    if destination_type == "jmap":
        after_items = [item for item in _query_jmap_items("calendars", file_path.stem) if item.uid == uid]
        _item, event, _collection = _jmap_object("calendars", file_path.stem, uid)
        overrides = event["recurrenceOverrides"]
        assert event["title"] == "Weekly planning changed"
        assert any(value.get("excluded") for value in overrides.values())
    else:
        after_items = [
            item for item in _dav_items(destination, "calendars", file_path.stem)[0] if item.uid == uid
        ]
        assert b"SUMMARY:Weekly planning changed" in after_items[0].data
        assert b"EXDATE:" in after_items[0].data
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert len(after_items) == len(before_items) == 1
    if destination_type == "jmap":
        _destroy_jmap_object("calendars", file_path.stem, uid, delete_collection=True)
