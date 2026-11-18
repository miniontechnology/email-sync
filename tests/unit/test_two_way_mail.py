import json

import pytest

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


def add_destination_only(state, key, folder="Work", raw=None):
    raw = raw or key.encode()
    state.destination.setdefault(folder, []).append(key)
    state.destination_raw[key] = raw
    state.raw_keys[raw] = key


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
    state.messages["Work"] = []
    assert run_migration(store, job_id, state).status == "completed"

    raw = b"new destination message"
    add_destination_only(state, "mid:new", raw=raw)
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


def test_copyback_append_failure_retries_after_source_search_miss_without_trashing_destination(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])], "Work": []})
    assert run_migration(store, job_id, state).status == "completed"
    add_destination_only(state, "mid:retry")
    state.append_failures = 1

    failed = run_migration(store, job_id, state)
    assert failed.status == "completed_with_errors"
    assert store.pairs_get_all(job_id)["mid:retry"]["state"] == "copying_back"
    assert state.messages["Work"] == []
    assert state.destination.get("Trash") is None

    retried = run_migration(store, job_id, state)
    assert retried.status == "completed"
    assert [message.key for message in state.messages["Work"]] == ["mid:retry"]
    assert store.pairs_get_all(job_id)["mid:retry"]["state"] == "copying_back"
    assert state.destination.get("Trash") is None

    resumed = run_migration(store, job_id, state)
    assert resumed.status == "completed"
    assert store.pairs_get_all(job_id)["mid:retry"]["state"] == "synced"
    assert state.destination.get("Trash") is None
    assert state.has_message_calls == 2


def test_copyback_search_hit_marks_pair_ignored_without_appending_or_trashing(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])], "Work": []})
    assert run_migration(store, job_id, state).status == "completed"
    add_destination_only(state, "mid:already-in-source")
    store.pairs_put_many(
        job_id,
        [
            {
                "key": "mid:already-in-source",
                "state": "copying_back",
                "src_folders": [],
                "src_flags": [],
                "dst_folders": ["Work"],
                "dst_flags": [],
            }
        ],
    )
    state.has_message_override = True

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert store.pairs_get_all(job_id)["mid:already-in-source"]["state"] == "ignored"
    assert state.messages["Work"] == []
    assert state.destination["Work"] == ["mid:already-in-source"]
    assert state.destination.get("Trash") is None


def test_copyback_resume_keeps_recorded_destination_primary_folder(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:resume-primary", b"primary", ["Work"])
    state = FakeMailState({"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])], "Work": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].append("mid:resume-primary")
    store.pairs_put_many(
        job_id,
        [
            {
                "key": "mid:resume-primary",
                "state": "copying_back",
                "src_folders": ["Work"],
                "src_flags": [],
                "dst_folders": ["Work"],
                "dst_flags": [],
            }
        ],
    )

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert store.pairs_get_all(job_id)["mid:resume-primary"]["dst_folders"] == ["Work"]
    assert state.destination.get("Trash") is None


def test_copyback_without_message_id_stays_pending_and_warns_once(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState({"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])], "Work": []})
    assert run_migration(store, job_id, state).status == "completed"
    key = "fp:" + "a" * 32
    add_destination_only(state, key)
    store.pairs_put_many(
        job_id,
        [
            {
                "key": key,
                "state": "copying_back",
                "src_folders": [],
                "src_flags": [],
                "dst_folders": ["Work"],
                "dst_flags": [],
            }
        ],
    )

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert store.pairs_get_all(job_id)[key]["state"] == "copying_back"
    assert state.messages["Work"] == []
    assert state.has_message_calls == 0
    warnings = [
        event["message"]
        for event in store.get_events(job_id)
        if "without Message-ID were not retried" in event["message"]
    ]
    assert len(warnings) == 1
    assert state.destination.get("Trash") is None


@pytest.mark.parametrize(
    ("target", "exclude"),
    [("Work", []), ("Work", ["Work"]), ("Junk", [])],
)
def test_copyback_unscanned_or_junk_target_is_left_alone_with_summary(tmp_path, target, exclude):
    store, job_id = make_store(tmp_path)
    messages = {"INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])]}
    if target == "Junk" or exclude:
        messages[target] = []
    state = FakeMailState(messages)
    assert run_migration(store, job_id, state).status == "completed"
    add_destination_only(state, "mid:unscanned", target)
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=False,
        workers=1,
        max_deletion_percent=100,
        exclude_folders=exclude,
    )

    result = run_migration(store, job_id, state, options)

    assert result.status == "completed"
    assert "mid:unscanned" not in store.pairs_get_all(job_id)
    assert state.destination[target] == ["mid:unscanned"]
    assert not any(message.key == "mid:unscanned" for folder in state.messages.values() for message in folder)
    summaries = [
        event["message"]
        for event in store.get_events(job_id)
        if "destination-only message(s) are in folders that aren't synced back" in event["message"]
    ]
    assert summaries == ["1 destination-only message(s) are in folders that aren't synced back; left alone"]
    assert state.destination.get("Trash") is None


def test_excluding_synced_source_folder_drops_pair_without_trashing(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:excluded-work", b"work", ["Work"])
    state = FakeMailState(
        {
            "INBOX": [FakeMail("mid:seed", b"seed", ["INBOX"])],
            "Work": [message],
        }
    )
    assert run_migration(store, job_id, state).status == "completed"
    assert "mid:excluded-work" in store.pairs_get_all(job_id)
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=False,
        workers=1,
        max_deletion_percent=100,
        exclude_folders=["Work"],
    )

    result = run_migration(store, job_id, state, options)

    assert result.status == "completed"
    assert "mid:excluded-work" not in store.pairs_get_all(job_id)
    assert state.destination["Work"] == ["mid:excluded-work"]
    assert state.messages["Work"] == [message]
    assert state.destination.get("Trash") is None
    assert state.messages.get("Trash") is None
    summaries = [
        event["message"]
        for event in store.get_events(job_id)
        if "left out of the current source scope" in event["message"]
    ]
    assert summaries == [
        "1 message(s) left out of the current source scope; tracking dropped without trashing"
    ]


def test_flag_conflict_sets_the_flag_on_both_sides(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:flag-conflict", b"flags", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    pair = store.pairs_get_all(job_id)["mid:flag-conflict"]
    store.pairs_put_many(
        job_id,
        [
            {
                **pair,
                "src_flags": [],
                "dst_flags": ["\\seen"],
            }
        ],
    )
    message.flags = ("\\Seen",)
    state.dest_flags[("INBOX", "mid:flag-conflict")] = frozenset()

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert "\\seen" in {flag.lower() for flag in message.flags}
    assert "\\seen" in {flag.lower() for flag in state.dest_flags[("INBOX", "mid:flag-conflict")]}


def test_folder_conflict_uses_source_folders(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:folder-conflict", b"folders", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    state.messages["INBOX"].remove(message)
    state.messages["Work"] = [message]
    state.destination["INBOX"].remove("mid:folder-conflict")
    state.destination["Other"] = ["mid:folder-conflict"]

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert [item.key for item in state.messages["Work"]] == ["mid:folder-conflict"]
    assert state.destination["Work"] == ["mid:folder-conflict"]
    assert state.destination["Other"] == []


def test_ambiguous_source_key_is_skipped_with_warning(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState(
        {
            "INBOX": [
                FakeMail("mid:ambiguous", b"one", ["INBOX"]),
                FakeMail("mid:ambiguous", b"two", ["INBOX"]),
            ]
        }
    )

    baseline = run_migration(store, job_id, state)
    assert baseline.status == "completed"
    before_destination = {folder: list(keys) for folder, keys in state.destination.items()}
    before_pairs = store.pairs_get_all(job_id)

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert store.pairs_get_all(job_id)["mid:ambiguous"]["state"] == "synced"
    assert state.destination == before_destination
    assert store.pairs_get_all(job_id) == before_pairs
    assert any(
        "Skipped 1 ambiguous two-way message key(s)" in event["message"] for event in store.get_events(job_id)
    )


def test_percent_threshold_pauses_for_deletion_in_folder_with_twenty_messages(tmp_path):
    store, job_id = make_store(tmp_path)
    messages = [FakeMail(f"mid:threshold-{number}", str(number).encode(), ["INBOX"]) for number in range(20)]
    state = FakeMailState({"INBOX": messages})
    assert run_migration(store, job_id, state).status == "completed"
    state.messages["INBOX"].remove(messages[0])
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=False,
        workers=1,
        max_deletions=100,
        max_deletion_percent=1,
    )

    result = run_migration(store, job_id, state, options)

    assert result.status == "paused"
    assert [item["key"] for item in store.get_job(job_id)["pending_deletions"]] == ["mid:threshold-0"]
    assert len(state.destination["INBOX"]) == 20
    assert state.destination.get("Trash") is None


def test_percent_threshold_counts_every_folder_for_multifolder_deletion(tmp_path):
    store, job_id = make_store(tmp_path)
    shared_inbox_copy = FakeMail("mid:multi-folder", b"shared", ["INBOX", "Work"])
    shared_work_copy = FakeMail("mid:multi-folder", b"shared", ["INBOX", "Work"])
    work_messages = [shared_work_copy] + [
        FakeMail(f"mid:work-{number}", str(number).encode(), ["Work"]) for number in range(19)
    ]
    state = FakeMailState({"INBOX": [shared_inbox_copy], "Work": work_messages})
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].remove("mid:multi-folder")
    state.destination["Work"].remove("mid:multi-folder")
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=False,
        workers=1,
        max_deletions=100,
        max_deletion_percent=1,
    )

    result = run_migration(store, job_id, state, options)

    assert result.status == "paused"
    assert [item["key"] for item in store.get_job(job_id)["pending_deletions"]] == ["mid:multi-folder"]
    assert state.destination.get("Trash") is None


def test_missing_source_trash_keeps_pair_and_does_not_expunge(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:no-source-trash", b"keep", ["INBOX"])
    state = FakeMailState({"INBOX": [message, FakeMail("mid:other", b"other", ["INBOX"])]})
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].remove("mid:no-source-trash")
    state.source_trash_enabled = False

    result = run_migration(store, job_id, state)

    assert result.status == "completed_with_errors"
    assert store.pairs_get_all(job_id)["mid:no-source-trash"]["state"] == "synced"
    assert state.messages["INBOX"][0] is message
    assert state.destination.get("Trash") is None
    assert any("Source has no Trash folder" in event["message"] for event in store.get_events(job_id))


def test_verify_does_not_fail_or_recopy_preexisting_destination_deletion(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:verify-deleted", b"keep", ["INBOX"])
    state = FakeMailState({"INBOX": [message, FakeMail("mid:other", b"other", ["INBOX"])]})
    assert run_migration(store, job_id, state).status == "completed"
    initial_uploads = state.upload_count
    state.destination["INBOX"].remove("mid:verify-deleted")
    state.source_trash_enabled = False
    options = Options(
        two_way=True,
        sync_interval_minutes=15,
        verify=True,
        workers=1,
        max_deletion_percent=100,
    )

    result = run_migration(store, job_id, state, options)

    ledger = next(
        row
        for row in store.ledger_items(job_id, "mail", limit=None)
        if row.get("match_key") == "mid:verify-deleted"
    )
    assert result.status == "completed_with_errors"
    assert ledger["status"] == "done"
    assert state.destination["INBOX"] == ["mid:other"]
    assert "mid:verify-deleted" not in state.destination["INBOX"]
    assert state.destination.get("Trash") is None
    assert state.upload_count == initial_uploads


def test_source_folder_removal_only_touches_that_folders_uid(tmp_path):
    store, job_id = make_store(tmp_path)
    state = FakeMailState(
        {
            "INBOX": [FakeMail("mid:folder-remove", b"same", ["INBOX"])],
            "Work": [FakeMail("mid:folder-remove", b"same", ["Work"])],
        }
    )
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].remove("mid:folder-remove")

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.source_removed_locations == [("INBOX", 1)]
    assert state.messages["INBOX"] == []
    assert [item.key for item in state.messages["Work"]] == ["mid:folder-remove"]


def test_single_destination_folder_move_uses_source_move(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:atomic-move", b"move", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].remove("mid:atomic-move")
    state.destination["Work"] = ["mid:atomic-move"]

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.source_move_count == 1
    assert state.messages["INBOX"] == []
    assert [item.key for item in state.messages["Work"]] == ["mid:atomic-move"]
    assert state.destination["Work"] == ["mid:atomic-move"]


def test_single_destination_folder_move_falls_back_when_source_lacks_move(tmp_path):
    store, job_id = make_store(tmp_path)
    message = FakeMail("mid:copy-remove", b"copy", ["INBOX"])
    state = FakeMailState({"INBOX": [message]})
    assert run_migration(store, job_id, state).status == "completed"
    state.destination["INBOX"].remove("mid:copy-remove")
    state.destination["Work"] = ["mid:copy-remove"]
    state.source_move_enabled = False

    result = run_migration(store, job_id, state)

    assert result.status == "completed"
    assert state.source_move_count == 0
    assert state.messages["INBOX"] == []
    assert [item.key for item in state.messages["Work"]] == ["mid:copy-remove"]


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
