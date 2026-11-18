import json

from emailsync import db
from emailsync.jobs.control import RunControl
from emailsync.mail.pipeline import MailMigration
from emailsync.mail.source import ImapSource, Scope
from emailsync.options import Endpoint, JobConfig, Options
from tests.unit.fakes import FakeMail, FakeMailState, factories


def make_config(options=None):
    return JobConfig(
        "two-way test",
        {
            "mail": {
                "source": Endpoint("imap", host="source", username="alice"),
                "dest": Endpoint("imap", host="destination", username="alice"),
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        options
        or Options(two_way=True, sync_interval_minutes=15, verify=False, workers=1, max_deletion_percent=100),
    )


def run_migration(store, job_id, state, options=None):
    source_factory, dest_factory = factories(state)
    return MailMigration(
        store,
        job_id,
        make_config(options),
        RunControl(),
        source_factory=source_factory,
        dest_factory=dest_factory,
    ).run()


def make_store(tmp_path):
    store = db.Store(tmp_path / "twoway.db")
    return store, store.create_job("two-way test", {}, None)


def test_first_run_records_both_sides_without_pushing_state(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])]})
    state.destination["INBOX"] = ["mid:seed", "mid:destination-only"]
    state.dest_flags[("INBOX", "mid:seed")] = frozenset({"\\seen"})

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.dest_flags[("INBOX", "mid:seed")] == frozenset({"\\seen"})
    pairs = store.pairs_get_all(job_id)
    assert pairs["mid:seed"]["src_flags"] == []
    assert pairs["mid:seed"]["dst_flags"] == ["\\seen"]
    assert pairs["mid:destination-only"]["state"] == "ignored"
    assert store.get_job(job_id)["twoway_baseline_at"]


def test_changes_since_baseline_merge_read_and_flag_state_both_ways(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:state", b"state", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"

    message.flags = ("\\Flagged",)
    state.dest_flags[("INBOX", "mid:state")] = frozenset({"\\seen"})
    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.dest_flags[("INBOX", "mid:state")] == frozenset({"\\seen", "\\flagged"})
    assert set(state.messages["INBOX"][0].flags) == {"\\seen", "\\flagged"}
    task = store.get_tasks(job_id)[0]
    assert task["updated"] == 1
    assert task["already"] + task["copied"] + task["skipped"] + task["failed"] == task["total"]


def test_source_folder_move_is_mirrored_without_copying_a_second_message(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:move", b"move", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    state.messages["INBOX"].remove(message)
    state.messages["Work"] = [message]

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.destination["INBOX"] == []
    assert state.destination["Work"] == ["mid:move"]
    assert state.upload_count == 1


def test_destination_only_message_is_copied_back_once_and_resumed(tmp_path):
    store, job_id = make_store(tmp_path)
    source_message = FakeMail("mid:seed", b"seed", ["INBOX"])
    state = FakeMailState({"INBOX": [source_message]})
    assert run_migration(store, job_id, state).status == "completed"

    raw = b"new destination message"
    state.destination.setdefault("Work", []).append("mid:new")
    state.destination_raw["mid:new"] = raw
    state.raw_keys[raw] = "mid:new"
    state.dest_flags[("Work", "mid:new")] = frozenset({"\\seen"})
    first = run_migration(store, job_id, state)
    assert first.status == "completed"
    assert store.get_tasks(job_id)[0]["copied_back"] == 1
    assert [mail.key for mail in state.messages["Work"]] == ["mid:new"]
    assert store.pairs_get_all(job_id)["mid:new"]["state"] == "copying_back"

    second = run_migration(store, job_id, state)
    assert second.status == "completed"
    assert [mail.key for mail in state.messages["Work"]] == ["mid:new"]
    assert store.pairs_get_all(job_id)["mid:new"]["state"] == "synced"
    assert state.upload_count == 1


def test_deletion_safety_pauses_then_moves_only_to_trash_after_approval(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState(
        {
            "INBOX": [
                FakeMail("mid:delete", b"delete", ["INBOX"]),
                FakeMail("mid:keep", b"keep", ["INBOX"]),
            ]
        }
    )
    assert run_migration(store, job_id, state).status == "completed"
    state.messages["INBOX"].remove(state.messages["INBOX"][0])

    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=False,
        workers=1,
        max_deletions=0,
        max_deletion_percent=100,
    )
    paused = run_migration(store, job_id, state, options)
    pending = store.get_job(job_id)["pending_deletions"]
    assert paused.status == "paused"
    assert [item["key"] for item in pending] == ["mid:delete"]
    assert state.destination["INBOX"] == ["mid:delete", "mid:keep"]
    assert state.destination.get("Trash") is None

    store.update_job(
        job_id,
        approved_deletions=json.dumps(["mid:delete"]),
        pending_deletions=None,
    )
    applied = run_migration(store, job_id, state, options)
    assert applied.status == "completed"
    assert state.destination["INBOX"] == ["mid:keep"]
    assert state.destination["Trash"] == ["mid:delete"]
    assert store.get_job(job_id)["approved_deletions"] == []
    assert store.get_tasks(job_id)[0]["trashed"] == 1


def test_empty_source_guard_skips_deletions(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:empty", b"empty", ["INBOX"])]})
    assert run_migration(store, job_id, state).status == "completed"
    state.messages["INBOX"].clear()

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.destination["INBOX"] == ["mid:empty"]
    assert state.destination.get("Trash") is None
    assert any("looks empty" in event["message"] for event in store.get_events(job_id))


def test_dry_run_without_baseline_does_not_record_pairs_or_marker(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:dry", b"dry", ["INBOX"])]})
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        dry_run=True,
        verify=False,
        workers=1,
    )

    result = run_migration(store, job_id, state, options)

    assert result.status == "completed"
    assert store.pairs_get_all(job_id) == {}
    assert store.get_job(job_id)["twoway_baseline_at"] is None
    assert state.destination == {}


def test_gmail_starred_reads_as_flagged_and_flagged_writes_back_symmetrically():
    class Connection:
        flags = None

        def ensure_selected(self, _scope):
            pass

        def fetch_state(self, _uids, gmail=False):
            assert gmail
            return {7: {"flags": (), "labels": ["\\Starred"]}}

        def ensure_writable(self, _scope):
            pass

        def add_flags(self, _uids, flags):
            self.flags = flags

    source = ImapSource(
        Endpoint("gmail", host="imap.gmail.com", username="alice"),
        Options(),
        gmail=True,
    )
    source.conn = Connection()
    state = source.fetch_state(Scope("All Mail", gmail_labels=True), [7])[7]

    assert "\\flagged" in {flag.lower() for flag in state.flags}
    source.set_flags([("All Mail", 7)], ["\\flagged"], [])
    assert source.conn.flags == ["\\Flagged"]
