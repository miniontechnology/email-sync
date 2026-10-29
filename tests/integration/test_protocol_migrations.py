from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from imapclient import IMAPClient

from emailsync import db
from emailsync.connectors.dav import DavClient
from emailsync.connectors.imap import ssl_context
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.pipeline import DavMigration
from emailsync.dav.vobjects import ensure_vcard_uid
from emailsync.jobs.control import RunControl
from emailsync.mail.dest import make_dest
from emailsync.mail.pipeline import MailMigration
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


def test_file_contacts_to_stalwart_jmap_is_idempotent(tmp_path):
    file_path = tmp_path / "contacts-jmap.vcf"
    second_card = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Two Person\r\nEMAIL:two@example.org\r\nEND:VCARD\r\n"
    second_uid, _card = ensure_vcard_uid(second_card)
    file_path.write_bytes(
        (
            "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:jmap-contact-1\r\nFN:One Person\r\n"
            "EMAIL:one@example.org\r\nEND:VCARD\r\n" + second_card
        ).encode()
    )
    config = _file_dav_config("contacts", file_path, _jmap_destination())
    store = db.Store(tmp_path / "contacts-jmap.db")
    job_id = store.create_job("JMAP contacts", {}, None)

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 2
    items = _query_jmap_items("contacts", "Default")
    expected_uids = {"jmap-contact-1", second_uid}
    assert expected_uids <= {item.uid for item in items}

    result = DavMigration(store, job_id, "contacts", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert expected_uids <= {item.uid for item in _query_jmap_items("contacts", "Default")}


def test_file_calendar_with_recurrence_to_stalwart_jmap_is_idempotent(tmp_path):
    file_path = tmp_path / "team-calendar-jmap.ics"
    file_path.write_bytes(
        b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Email Sync Tests//EN\r\n"
        b"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n"
        b"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0400\r\n"
        b"TZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n"
        b"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nTZOFFSETFROM:-0500\r\n"
        b"TZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n"
        b"BEGIN:VEVENT\r\nUID:jmap-weekly-1\r\nDTSTAMP:20240101T000000Z\r\n"
        b"DTSTART;TZID=America/New_York:20250106T090000\r\n"
        b"DTEND;TZID=America/New_York:20250106T100000\r\n"
        b"RRULE:FREQ=WEEKLY;COUNT=4\r\nSUMMARY:Weekly planning\r\nEND:VEVENT\r\n"
        b"BEGIN:VEVENT\r\nUID:jmap-weekly-1\r\n"
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
    assert [item.uid for item in items] == ["jmap-weekly-1"]
    assert items[0].fingerprint == ""

    result = DavMigration(store, job_id, "calendars", config, RunControl()).run()
    assert result.status == "completed", result.error
    assert store.get_tasks(job_id)[0]["copied"] == 0
    assert [item.uid for item in _query_jmap_items("calendars", file_path.stem)] == ["jmap-weekly-1"]


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
    contact_data = (
        b"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:cross-protocol-contact-1\r\n"
        b"FN:Shared Person\r\nEMAIL:shared@example.org\r\nEND:VCARD\r\n"
    )
    try:
        _home, collections = carddav.discover()
        assert collections
        default_book = collections[0]
        assert carddav.put(default_book, "cross-protocol-contact-1.vcf", contact_data) is True
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
