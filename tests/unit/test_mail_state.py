from emailsync.mail.index import DestIndex
from emailsync.mail.pipeline import MailMigration
from emailsync.mail.state import DestCopy, imap_flag, normalize_flags, plan_change
from emailsync.options import Options


def test_normalize_flags_keeps_only_syncable_system_flags_and_keywords():
    assert normalize_flags(
        [
            "\\Seen",
            "\\Flagged",
            "\\Answered",
            "\\Draft",
            "\\Recent",
            "\\Deleted",
            "\\Other",
            "$Forwarded",
            "bad value",
            "bad*value",
            "x" * 256,
        ]
    ) == frozenset({"\\seen", "\\flagged", "\\answered", "\\draft", "$forwarded"})
    assert imap_flag("\\seen") == "\\Seen"
    assert imap_flag("\\flagged") == "\\Flagged"
    assert imap_flag("\\answered") == "\\Answered"
    assert imap_flag("\\draft") == "\\Draft"
    assert imap_flag("$forwarded") == "$forwarded"


def test_plan_change_applies_only_source_flag_deltas():
    copy = DestCopy(
        "INBOX:1:1",
        frozenset({"INBOX"}),
        frozenset({"\\seen", "$destination-only"}),
    )
    change = plan_change(
        "source",
        [copy],
        {"INBOX"},
        frozenset({"\\seen"}),
        frozenset({"\\flagged"}),
        ["INBOX"],
        ["INBOX"],
        False,
        False,
    )
    assert change.flag_ops == [(copy, frozenset({"\\flagged"}), frozenset({"\\seen"}))]


def test_missing_flag_baseline_is_add_only():
    copy = DestCopy("INBOX:1:1", frozenset({"INBOX"}), frozenset({"\\seen", "$local"}))
    change = plan_change("source", [copy], {"INBOX"}, None, frozenset(), ["INBOX"], ["INBOX"], False, False)
    assert change.flag_ops == []


def test_label_changes_keep_removed_folders_by_default_and_never_remove_last_copy():
    copy = DestCopy("INBOX:1:1", frozenset({"INBOX"}), frozenset())
    keep = plan_change(
        "source",
        [copy],
        {"INBOX"},
        frozenset(),
        frozenset(),
        ["INBOX", "Work"],
        ["INBOX"],
        False,
        False,
    )
    assert keep.remove_folders == []

    remove = plan_change(
        "source",
        [copy],
        {"INBOX"},
        frozenset(),
        frozenset(),
        ["INBOX"],
        [],
        False,
        True,
    )
    assert remove.remove_folders == []


def test_label_add_and_stale_removal():
    copy = DestCopy("INBOX:1:1", frozenset({"INBOX"}), frozenset())
    add = plan_change(
        "source", [copy], {"INBOX"}, frozenset(), frozenset(), ["INBOX"], ["INBOX", "Work"], False, False
    )
    assert add.add_folders == ["Work"]
    remove = plan_change(
        "source",
        [copy, DestCopy("Work:1:1", frozenset({"Work"}), frozenset())],
        {"INBOX", "Work"},
        frozenset(),
        frozenset(),
        ["INBOX", "Work"],
        ["INBOX"],
        False,
        True,
    )
    assert remove.remove_folders == ["Work"]


def test_archived_gmail_uses_fallback_only_in_remove_mode():
    inbox = DestCopy("INBOX:1:1", frozenset({"INBOX"}), frozenset())
    keep = plan_change(
        "source", [inbox], {"INBOX"}, frozenset(), frozenset(), ["INBOX"], ["Archive"], True, False
    )
    assert keep.add_folders == []
    assert keep.remove_folders == []

    remove = plan_change(
        "source", [inbox], {"INBOX"}, frozenset(), frozenset(), ["INBOX"], ["Archive"], True, True
    )
    assert remove.add_folders == ["Archive"]
    assert remove.remove_folders == ["INBOX"]


def test_ambiguous_folder_presence_does_not_trigger_another_add():
    ambiguous = [
        DestCopy("INBOX:1:1", frozenset({"INBOX"}), frozenset()),
        DestCopy("INBOX:1:2", frozenset({"INBOX"}), frozenset()),
    ]
    assert MailMigration._select_copies(ambiguous, None) == []
    change = plan_change(
        "source",
        [],
        {"INBOX"},
        frozenset(),
        frozenset(),
        ["INBOX"],
        ["INBOX", "Work"],
        False,
        False,
    )
    assert change.add_folders == ["Work"]
    assert "INBOX" not in change.add_folders
    assert Options.from_dict({}).sync_flags_labels is True
    assert Options.from_dict({}).remove_stale_labels is False


def test_destination_index_retains_copy_refs_flags_and_legacy_ref_for():
    index = DestIndex()
    index.add_ref("mid:one", "INBOX:5:12", ["INBOX"], ["\\Seen", "$Forwarded"])
    assert index.ref_for("mid:one") == ("INBOX:5:12", frozenset({"INBOX"}))
    assert index.copies("mid:one") == [
        DestCopy("INBOX:5:12", frozenset({"INBOX"}), frozenset({"\\seen", "$forwarded"}))
    ]


def test_ledger_baseline_parser_falls_back_for_invalid_info():
    assert MailMigration._read_baseline({"info": "{invalid", "targets": ["INBOX"]}) == (
        None,
        ["INBOX"],
    )
    assert MailMigration._read_baseline({"info": '{"src_targets":["Work"]}', "targets": ["INBOX"]}) == (
        None,
        ["Work"],
    )
