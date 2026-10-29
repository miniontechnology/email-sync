from emailsync.crypto import SecretBox
from emailsync.db import DONE, FAILED, Store
from emailsync.errors import (
    AuthError,
    FatalError,
    PermanentError,
    ThrottledError,
    TransientError,
    backoff,
    classify,
)
from emailsync.mail.fingerprint import key_from_jmap, normalize_message_id, summary_from_headers
from emailsync.mail.folders import Folder, gmail_targets, map_source_folder, parse_folder_map
from emailsync.mail.index import DestIndex
from emailsync.options import Endpoint, JobConfig, Options


def test_fingerprint_normalizes_message_id_and_jmap_key_matches_headers():
    assert normalize_message_id(" <one@EXAMPLE> ") == "one@EXAMPLE"
    header = (
        b"Message-ID: <msg@example.test>\r\nDate: Tue, 01 Jan 2019 12:00:00 +0000\r\n"
        b"From: Person <p@example.test>\r\nTo: x@example.test\r\nSubject: Test\r\n\r\n"
    )
    jmap = {
        "messageId": ["<msg@example.test>"],
        "sentAt": "2019-01-01T12:00:00Z",
        "from": [{"email": "p@example.test"}],
        "to": [{"email": "x@example.test"}],
        "subject": "Test",
    }
    assert summary_from_headers(header)["key"] == key_from_jmap(jmap)


def test_fingerprint_decodes_subject_and_falls_back_without_message_id():
    header = (
        b"Date: Tue, 01 Jan 2019 12:00:00 +0000\r\nFrom: Person <p@example.test>\r\n"
        b"To: x@example.test\r\nSubject: =?utf-8?b?SMOpbGxv?=\r\n\r\n"
    )
    summary = summary_from_headers(header)
    assert summary["subject"] == "Héllo"
    assert summary["key"].startswith("fp:")


def test_folder_mapping_and_gmail_targets():
    options = Options(folder_map={"Old": "New"}, exclude_folders=["Blocked"], archive_target="@archive")
    assert map_source_folder(Folder("INBOX.Work", "."), options) == "Work"
    assert map_source_folder(Folder("Old"), options) == "New"
    assert map_source_folder(Folder("Blocked"), options) is None
    assert gmail_targets(["\\Inbox", "\\Starred", "Projects"], options) == (["@inbox", "Projects"], True)
    assert gmail_targets([], options)[0] == ["@archive"]
    assert parse_folder_map("Old => New\nSkip =>\n# ignored") == {"Old": "New", "Skip": ""}


def test_dest_index_counts_duplicate_keys_per_folder_and_account():
    index = DestIndex()
    index.add("Inbox", "mid:x")
    assert index.decide("mid:x", ["Inbox"]) == []
    assert index.decide("mid:x", ["Inbox"]) == ["Inbox"]
    account = DestIndex("account")
    account.add("Inbox", "mid:x")
    assert account.decide("mid:x", ["Archive"]) == []
    assert account.decide("mid:x", ["Archive"]) == ["Archive"]


def test_error_classification_and_backoff():
    assert classify(TimeoutError()) is TransientError
    assert classify(RuntimeError("Login failed")) is AuthError
    assert classify(RuntimeError("mailbox full")) is FatalError
    assert classify(RuntimeError("bad message")) is PermanentError
    assert classify(RuntimeError("rate limit")) is ThrottledError
    assert [backoff(i) for i in range(1, 5)] == [2, 4, 8, 16]


def test_options_split_secret_roundtrip_and_validation(tmp_path):
    config = JobConfig(
        "Migration",
        {
            "mail": {
                "source": Endpoint("imap", host="source", username="alice", password="secret"),
                "dest": Endpoint("jmap", url="https://dest", username="bob", token="bearer"),
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(),
    )
    public, secrets = config.split()
    assert "password" not in public["endpoints"]["mail"]["source"]
    assert "token" not in public["endpoints"]["mail"]["dest"]
    box = SecretBox.from_settings("test-key", tmp_path)
    restored = JobConfig.from_dict(public, box.decrypt(box.encrypt(secrets)))
    assert restored.endpoints["mail"]["source"].password == "secret"
    assert restored.endpoints["mail"]["dest"].token == "bearer"
    assert restored.validate() == []


def test_db_ledger_transitions_count_attempts(tmp_path):
    store = Store(tmp_path / "test.db")
    job_id = store.create_job("job", {}, None)
    store.ledger_mark(
        job_id, "mail", "INBOX|1|1", "uploading", match_key="mid:x", targets=["INBOX"], count_attempt=True
    )
    store.ledger_mark(job_id, "mail", "INBOX|1|1", DONE, dest_ref="ref")
    store.ledger_mark(job_id, "mail", "INBOX|1|2", FAILED, error="bad")
    row = store.ledger_get_many(job_id, "mail", ["INBOX|1|1"])["INBOX|1|1"]
    assert row["status"] == DONE
    assert row["targets"] == ["INBOX"]
    assert row["attempts"] == 1
    assert store.ledger_failed(job_id)[0]["error"] == "bad"
