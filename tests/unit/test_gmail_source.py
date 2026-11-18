from imapclient.imap_utf7 import encode

from emailsync.connectors.imap import decode_label
from emailsync.mail.source import ImapSource
from emailsync.options import Endpoint, Options


class FakeImap:
    def __init__(self):
        self.labels = ["\\Inbox", "Projects/Planning", "\\Starred"]
        self.calls = []
        self.selected = []
        self.search_results = [1]

    def ensure_selected(self, name):
        self.selected.append(name)

    def has(self, capability):
        return capability in ("X-GM-EXT-1", "MOVE")

    def list_folders(self):
        from emailsync.mail.folders import Folder

        return [
            Folder("[Gmail]/All Mail", "/", ("\\All",)),
            Folder("INBOX", "/", ("\\Inbox",)),
            Folder("[Gmail]/Spam", "/", ("\\Junk",)),
            Folder("[Gmail]/Trash", "/", ("\\Trash",)),
        ]

    def select(self, name, readonly=True):
        return {"uidvalidity": 1}

    def create_folder(self, name):
        self.calls.append(("create_folder", name))

    def ensure_writable(self, name):
        self.calls.append(("ensure_writable", name))

    def search_header(self, name, value):
        self.calls.append(("search_header", name, value))
        return self.search_results

    def add_gmail_labels(self, uids, labels):
        self.calls.append(("add_gmail_labels", uids, labels))

    def remove_gmail_labels(self, uids, labels):
        self.calls.append(("remove_gmail_labels", uids, labels))

    def move(self, uids, folder):
        self.calls.append(("move", uids, folder))

    def search_uids(self, since=None):
        return [1]

    def fetch_meta(self, _uids, gmail=False):
        assert gmail
        return {
            1: {
                "header": b"Message-ID: <gmail@example.test>\r\nSubject: Hi\r\n\r\n",
                "flags": (),
                "labels": self.labels,
                "internaldate": None,
                "size": 3,
            }
        }

    def fetch_state(self, _uids, gmail=False):
        return {1: {"flags": (), "labels": self.labels if gmail else []}}


def test_gmail_uses_all_mail_labels_and_starred_flag():
    source = ImapSource(Endpoint("gmail", host="imap.gmail.com"), Options(), gmail=True)
    source.conn = FakeImap()
    scopes = source.scopes()
    assert [scope.name for scope in scopes] == ["[Gmail]/All Mail"]
    assert source.open_scope(scopes[0]) == (1, [1])
    meta = source.fetch_meta(scopes[0], [1])[1]
    assert meta.targets == ["@inbox", "Projects/Planning"]
    assert meta.flags == ("\\Flagged",)
    assert meta.archived is False

    source_state = source.fetch_state(scopes[0], [1])[1]
    assert source_state.targets == ["@inbox", "Projects/Planning"]
    assert source_state.flags == ("\\Flagged",)
    assert source_state.archived is False

    generic = ImapSource(Endpoint("imap", host="imap.example.test"), Options())
    generic.conn = FakeImap()
    scope = next(scope for scope in generic.scopes() if scope.name == "INBOX")
    assert generic.fetch_state(scope, [1])[1].targets == [scope.target]


def test_gmail_archived_state_uses_archive_fallback():
    source = ImapSource(
        Endpoint("gmail", host="imap.gmail.com"),
        Options(archive_target="@archive"),
        gmail=True,
    )
    source.conn = FakeImap()
    source.conn.labels = []
    source_state = source.fetch_state(source.scopes()[0], [1])[1]
    assert source_state.targets == ["@archive"]
    assert source_state.archived is True
    meta = source.fetch_meta(source.scopes()[0], [1])[1]
    assert meta.targets == ["@archive"]
    assert meta.archived is True


def test_gmail_optional_spam_trash_and_modified_utf7_labels():
    source = ImapSource(
        Endpoint("gmail", host="imap.gmail.com"), Options(include_spam=True, include_trash=True), gmail=True
    )
    source.conn = FakeImap()
    assert [scope.name for scope in source.scopes()] == ["[Gmail]/All Mail", "[Gmail]/Spam", "[Gmail]/Trash"]
    assert decode_label(encode("Projet été")) == "Projet été"


def test_gmail_two_way_folder_labels_archive_and_trash_use_gmail_operations():
    source = ImapSource(
        Endpoint("gmail", host="imap.gmail.com"),
        Options(two_way=True),
        gmail=True,
    )
    connection = FakeImap()
    source.conn = connection

    source.add_folder(("[Gmail]/All Mail", 7), "Projects")
    source.remove_folder(("[Gmail]/All Mail", 7), "Old")
    source.remove_folder(("[Gmail]/All Mail", 7), "@inbox")
    source.to_trash([("[Gmail]/All Mail", 7), ("[Gmail]/Trash", 8)])

    assert ("add_gmail_labels", [7], ["Projects"]) in connection.calls
    assert ("remove_gmail_labels", [7], ["Old"]) in connection.calls
    assert ("remove_gmail_labels", [7], ["\\Inbox"]) in connection.calls
    assert ("move", [7], "[Gmail]/Trash") in connection.calls
    assert ("move", [8], "[Gmail]/Trash") not in connection.calls


def test_has_message_searches_message_id_in_target_or_gmail_all_mail():
    imap = ImapSource(Endpoint("imap", host="imap.example.test"), Options())
    imap_connection = FakeImap()
    imap.conn = imap_connection
    assert imap.has_message("Work", "mid:one@example.test") is True
    assert imap_connection.selected == ["Work"]
    assert ("search_header", "Message-ID", "<one@example.test>") in imap_connection.calls

    gmail = ImapSource(
        Endpoint("gmail", host="imap.gmail.com"),
        Options(),
        gmail=True,
    )
    gmail_connection = FakeImap()
    gmail.conn = gmail_connection
    assert gmail.has_message("Projects", "mid:two@example.test") is True
    assert gmail_connection.selected == ["[Gmail]/All Mail"]
    assert ("search_header", "Message-ID", "<two@example.test>") in gmail_connection.calls


def test_imap_source_uses_move_for_a_folder_move_when_supported():
    source = ImapSource(Endpoint("imap", host="imap.example.test"), Options())
    connection = FakeImap()
    source.conn = connection

    assert source.move_folder(("INBOX", 9), "Work") is True

    assert ("create_folder", "Work") in connection.calls
    assert ("ensure_writable", "INBOX") in connection.calls
    assert ("move", [9], "Work") in connection.calls


def test_imap_source_to_trash_skips_locations_already_in_trash():
    source = ImapSource(Endpoint("imap", host="imap.example.test"), Options(two_way=True))
    connection = FakeImap()
    source.conn = connection

    source.to_trash([("INBOX", 3), ("[Gmail]/Trash", 4)])

    assert ("move", [3], "[Gmail]/Trash") in connection.calls
    assert ("move", [4], "[Gmail]/Trash") not in connection.calls
