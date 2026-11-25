from emailsync.mail.folders import Folder, folder_rows
from emailsync.options import Options


def test_folder_rows_for_generic_imap_roles_exclusions_and_mappings():
    folders = [
        Folder("INBOX"),
        Folder("Sent Items", flags=("\\Sent",)),
        Folder("Drafts", flags=("\\Drafts",)),
        Folder("INBOX.Work", delimiter="."),
        Folder("All Mail", flags=("\\All",)),
        Folder("Important", flags=("\\Important",)),
        Folder("Flagged", flags=("\\Flagged",)),
        Folder("Spam", flags=("\\Junk",)),
        Folder("Trash", flags=("\\Trash",)),
        Folder("Hidden", flags=("\\NoSelect",)),
    ]
    options = Options(
        include_spam=True,
        exclude_folders=["INBOX.Work", "Gone"],
        folder_map={"Sent Items": "Archive/Sent", "Missing": "Moved"},
    )

    rows = folder_rows(folders, options, gmail=False)
    by_key = {row["key"]: row for row in rows}

    assert [row["name"] for row in rows[:3]] == ["INBOX", "Sent Items", "Drafts"]
    assert by_key["INBOX"]["default_target"] == "@inbox"
    assert by_key["Sent Items"]["default_target"] == "@sent"
    assert by_key["Sent Items"]["dest"] == "Archive/Sent"
    assert by_key["INBOX.Work"]["default_target"] == "Work"
    assert by_key["INBOX.Work"]["included"] is False
    assert by_key["Spam"]["kind"] == "spam"
    assert by_key["Spam"]["included"] is True
    assert by_key["Trash"]["kind"] == "trash"
    assert by_key["Trash"]["included"] is False
    assert not {"All Mail", "Important", "Flagged", "Hidden"} & by_key.keys()
    assert by_key["Missing"]["missing"] is True
    assert by_key["Missing"]["dest"] == "Moved"
    assert by_key["Gone"]["missing"] is True
    assert [row["name"] for row in rows[-2:]] == ["Gone", "Missing"]


def test_folder_rows_for_gmail_labels_and_system_folders():
    folders = [
        Folder("[Gmail]/All Mail", flags=("\\All",)),
        Folder("[Gmail]/Starred", flags=("\\Flagged",)),
        Folder("INBOX"),
        Folder("[Gmail]/Sent Mail", flags=("\\Sent",)),
        Folder("[Gmail]/Drafts", flags=("\\Drafts",)),
        Folder("[Gmail]/Important", flags=("\\Important",)),
        Folder("[Gmail]/Spam"),
        Folder("[Gmail]/Trash", flags=("\\Trash",)),
        Folder("Projects"),
    ]
    options = Options(
        include_spam=True,
        gmail_important_as_folder=True,
        exclude_folders=["Projects", "Old Label"],
        folder_map={
            "\\Sent": "Sent Archive",
            "[Gmail]/Spam": "Bulk",
            "Missing Label": "Existing",
        },
    )

    rows = folder_rows(folders, options, gmail=True)
    by_key = {row["key"]: row for row in rows}

    assert [row["name"] for row in rows[:3]] == ["INBOX", "[Gmail]/Sent Mail", "[Gmail]/Drafts"]
    assert by_key["\\Inbox"]["default_target"] == "@inbox"
    assert by_key["\\Sent"]["default_target"] == "@sent"
    assert by_key["\\Sent"]["dest"] == "Sent Archive"
    assert by_key["\\Draft"]["default_target"] == "@drafts"
    assert by_key["\\Important"]["kind"] == "important"
    assert by_key["\\Important"]["default_target"] == "Important"
    assert by_key["\\Important"]["included"] is True
    assert by_key["[Gmail]/Spam"]["kind"] == "spam"
    assert by_key["[Gmail]/Spam"]["dest"] == "Bulk"
    assert by_key["[Gmail]/Spam"]["default_target"] == "@junk"
    assert by_key["[Gmail]/Trash"]["kind"] == "trash"
    assert by_key["[Gmail]/Trash"]["default_target"] == "@trash"
    assert by_key["[Gmail]/Trash"]["included"] is False
    assert by_key["Projects"]["included"] is False
    assert not {"[Gmail]/All Mail", "[Gmail]/Starred"} & by_key.keys()
    assert by_key["Missing Label"]["missing"] is True
    assert by_key["Missing Label"]["dest"] == "Existing"
    assert by_key["Old Label"]["missing"] is True
    assert [row["name"] for row in rows[-2:]] == ["Missing Label", "Old Label"]
