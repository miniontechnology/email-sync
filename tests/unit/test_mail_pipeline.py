import json

from emailsync import db
from emailsync.jobs.control import RunControl
from emailsync.mail.pipeline import MailMigration
from emailsync.options import Endpoint, JobConfig, Options
from tests.unit.fakes import FakeMail, FakeMailState, factories


def make_config(options=None):
    return JobConfig(
        "fake migration",
        {
            "mail": {
                "source": Endpoint("imap", host="source"),
                "dest": Endpoint("imap", host="dest"),
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        options or Options(verify=False, workers=2),
    )


def run_migration(store, job_id, state, options=None):
    source_factory, dest_factory = factories(state)
    migration = MailMigration(
        store,
        job_id,
        make_config(options),
        RunControl(),
        source_factory=source_factory,
        dest_factory=dest_factory,
    )
    return migration.run()


def make_store(tmp_path):
    store = db.Store(tmp_path / "sync.db")
    return store, store.create_job("fake", {}, None)


def test_full_copy_then_rerun_is_idempotent(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"]), FakeMail("mid:b", b"b", ["INBOX"])]})
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination == {"INBOX": ["mid:a", "mid:b"]}
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination == {"INBOX": ["mid:a", "mid:b"]}
    task = store.get_tasks(job_id)[0]
    assert task["already"] == 2
    assert task["copied"] == 0
    assert task["updated"] == 0


def test_destination_preseeded_by_other_tool_copies_only_missing(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail(f"mid:{n}", str(n).encode(), ["INBOX"]) for n in range(4)]})
    state.destination["INBOX"] = ["mid:0", "mid:2"]
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination["INBOX"] == ["mid:0", "mid:2", "mid:1", "mid:3"]


def test_duplicate_message_ids_preserve_source_copy_count(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState(
        {
            "INBOX": [
                FakeMail("mid:duplicate", b"one", ["INBOX"]),
                FakeMail("mid:duplicate", b"two", ["INBOX"]),
                FakeMail("mid:duplicate", b"three", ["INBOX"]),
            ]
        }
    )
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination["INBOX"].count("mid:duplicate") == 3


def test_duplicate_message_ids_in_other_folders_keep_flag_updates_separate(tmp_path):
    store, job_id = make_store(tmp_path)
    inbox_message = FakeMail("mid:duplicate", b"inbox copy", ["INBOX"])
    work_message = FakeMail("mid:duplicate", b"work copy", ["Work"])
    state = FakeMailState({"INBOX": [inbox_message], "Work": [work_message]})
    assert run_migration(store, job_id, state).status == "completed"

    inbox_message.flags = ("\\Seen",)
    assert run_migration(store, job_id, state).status == "completed"
    assert state.dest_flags[("INBOX", "mid:duplicate")] == frozenset({"\\seen"})
    assert state.dest_flags[("Work", "mid:duplicate")] == frozenset()


def test_ambiguous_destination_copies_preserve_baseline_and_warn(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:ambiguous", b"body", ["INBOX"], flags=("\\Seen",))
    state = FakeMailState({"INBOX": [message]})
    state.destination["INBOX"] = ["mid:ambiguous", "mid:ambiguous"]

    assert run_migration(store, job_id, state).status == "completed"
    row = store.ledger_items(job_id, "mail")[0]
    assert row["status"] == db.SKIPPED
    assert row["info"] is None
    assert store.get_tasks(job_id)[0]["updated"] == 0
    assert any(
        "1 message(s) had ambiguous destination copies" in event["message"]
        for event in store.get_events(job_id)
    )


def test_multi_target_copies_one_source_to_each_folder(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"All Mail": [FakeMail("mid:labelled", b"mail", ["INBOX", "Work"])]})
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination == {"INBOX": ["mid:labelled"], "Work": ["mid:labelled"]}


def test_transient_source_failure_retries(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"])]})
    state.transient_fetches = 1
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.destination["INBOX"] == ["mid:a"]


def test_permanent_item_error_does_not_stop_other_items_and_retries_later(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"]), FakeMail("mid:b", b"b", ["INBOX"])]})
    state.upload_fail_after["mid:a"] = 0
    result = run_migration(store, job_id, state)
    assert result.status == "completed_with_errors"
    assert state.destination == {"INBOX": ["mid:b"]}
    assert run_migration(store, job_id, state).status == "completed"
    assert state.destination["INBOX"].count("mid:a") == 1


def test_dry_run_does_not_write_ledger_or_destination(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"])]})
    result = run_migration(store, job_id, state, Options(dry_run=True, verify=False))
    assert result.status == "completed"
    assert state.destination == {}
    assert store.ledger_counts(job_id, "mail") == {}


def test_verify_marks_disappeared_destination_item_failed(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"])]})
    state.drop_uploads = True
    result = run_migration(store, job_id, state, Options(verify=True, workers=1))
    assert result.status == "completed_with_errors"
    row = store.ledger_items(job_id, "mail")[0]
    assert row["status"] == db.FAILED
    assert row["error"] == "missing in destination after copy (verify)"


def test_resume_reconciles_uploading_ledger_against_destination(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"])]})
    state.destination["INBOX"] = ["mid:a"]
    store.ledger_mark(
        job_id, "mail", "INBOX|1|1", db.UPLOADING, match_key="mid:a", targets=["INBOX"], count_attempt=True
    )
    assert run_migration(store, job_id, state).status == "completed"
    assert state.destination["INBOX"] == ["mid:a"]
    row = store.ledger_items(job_id, "mail")[0]
    assert row["status"] == db.SKIPPED
    assert row["attempts"] == 1


def test_pause_and_cancel_return_without_creating_ledger_rows(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:a", b"a", ["INBOX"])]})
    source_factory, dest_factory = factories(state)
    control = RunControl()
    control.stop("pause")
    paused = MailMigration(store, job_id, make_config(), control, source_factory, dest_factory).run()
    assert paused.status == "paused"
    assert store.ledger_counts(job_id, "mail") == {}

    store, job_id = make_store(tmp_path / "cancel")
    control = RunControl()
    control.stop("cancel")
    cancelled = MailMigration(store, job_id, make_config(), control, source_factory, dest_factory).run()
    assert cancelled.status == "cancelled"
    assert store.ledger_counts(job_id, "mail") == {}


def test_settled_flags_sync_without_upload_and_updates_baseline(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:flags", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    row = store.ledger_items(job_id, "mail")[0]
    baseline = json.loads(row["info"])
    assert baseline == {"flags": [], "src_targets": ["INBOX"]}
    body_fetch_count = state.body_fetch_count

    message.flags = ("\\Seen", "\\Flagged")
    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert state.dest_flags[("INBOX", "mid:flags")] == frozenset({"\\seen", "\\flagged"})
    assert state.upload_count == 1
    assert state.body_fetch_count == body_fetch_count
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert json.loads(store.ledger_items(job_id, "mail")[0]["info"])["flags"] == ["\\flagged", "\\seen"]

    result = run_migration(store, job_id, state)
    assert result.status == "completed"
    assert store.get_tasks(job_id)[0]["updated"] == 0


def test_destination_only_flags_survive_source_flag_update(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:flags", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    run_migration(store, job_id, state)
    state.dest_flags[("INBOX", "mid:flags")] = frozenset({"$destination"})
    message.flags = ("\\Seen",)

    assert run_migration(store, job_id, state).status == "completed"
    assert state.dest_flags[("INBOX", "mid:flags")] == frozenset({"$destination", "\\seen"})


def test_labels_add_and_remove_keep_or_remove_without_reupload(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:labels", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    run_migration(store, job_id, state)

    message.targets = ["INBOX", "Work"]
    assert run_migration(store, job_id, state).status == "completed"
    assert state.destination == {"INBOX": ["mid:labels"], "Work": ["mid:labels"]}
    assert state.upload_count == 1
    assert store.ledger_items(job_id, "mail")[0]["targets"] == ["INBOX", "Work"]

    message.targets = ["INBOX"]
    assert run_migration(store, job_id, state).status == "completed"
    assert state.destination["Work"] == ["mid:labels"]
    assert state.upload_count == 1

    message.targets = ["INBOX", "Work"]
    assert run_migration(store, job_id, state).status == "completed"
    assert store.ledger_items(job_id, "mail")[0]["info"]

    message.targets = ["INBOX"]
    result = run_migration(store, job_id, state, Options(verify=False, remove_stale_labels=True, workers=1))
    assert result.status == "completed"
    assert state.destination["Work"] == []
    assert state.destination["INBOX"] == ["mid:labels"]
    assert state.upload_count == 1


def test_disabled_sync_and_dry_run_do_not_change_destination_or_baseline(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:flags", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    run_migration(store, job_id, state)
    row = store.ledger_items(job_id, "mail")[0]
    original_info = row["info"]

    message.flags = ("\\Seen",)
    message.targets = ["INBOX", "Work"]
    assert (
        run_migration(store, job_id, state, Options(verify=False, sync_flags_labels=False)).status
        == "completed"
    )
    assert state.dest_flags[("INBOX", "mid:flags")] == frozenset()
    assert state.destination.get("Work") is None
    assert store.ledger_items(job_id, "mail")[0]["targets"] == ["INBOX"]
    assert store.ledger_items(job_id, "mail")[0]["info"] == original_info

    result = run_migration(store, job_id, state, Options(verify=False, dry_run=True))
    assert result.status == "completed"
    assert store.get_tasks(job_id)[0]["updated"] == 1
    assert state.dest_flags[("INBOX", "mid:flags")] == frozenset()
    assert store.ledger_items(job_id, "mail")[0]["info"] == original_info


def test_preseeded_copy_gets_state_in_same_run(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:seeded", b"body", ["INBOX"], flags=("\\Seen",))
    state = FakeMailState({"INBOX": [message]})
    state.destination["INBOX"] = ["mid:seeded"]
    state.dest_flags[("INBOX", "mid:seeded")] = frozenset({"\\seen"})

    assert run_migration(store, job_id, state).status == "completed"
    row = store.ledger_items(job_id, "mail")[0]
    assert row["status"] == db.SKIPPED
    assert state.dest_flags[("INBOX", "mid:seeded")] == frozenset({"\\seen"})
    assert json.loads(row["info"]) == {"flags": ["\\seen"], "src_targets": ["INBOX"]}
    assert store.get_tasks(job_id)[0]["updated"] == 0
    assert state.upload_count == 0
    assert state.body_fetch_count == 0


def test_state_apply_failure_preserves_baseline_and_succeeds_next_run(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:failure", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    run_migration(store, job_id, state)
    row = store.ledger_items(job_id, "mail")[0]
    original_info = row["info"]
    message.flags = ("\\Seen",)
    state.apply_failures["INBOX|1|1"] = 1

    result = run_migration(store, job_id, state)
    assert result.status == "completed_with_errors"
    row = store.ledger_items(job_id, "mail")[0]
    assert row["status"] == db.DONE
    assert row["info"] == original_info
    assert row["error"] == "injected state update failure"

    assert run_migration(store, job_id, state).status == "completed"
    row = store.ledger_items(job_id, "mail")[0]
    assert json.loads(row["info"])["flags"] == ["\\seen"]
    assert row["error"] is None
    assert state.upload_count == 1


def test_missing_settled_copy_warns_without_reupload(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:missing", b"body", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    run_migration(store, job_id, state)
    state.destination["INBOX"].clear()

    assert run_migration(store, job_id, state).status == "completed"
    assert state.upload_count == 1
    assert any(
        "1 already-copied message(s) weren't found" in event["message"] for event in store.get_events(job_id)
    )
