from imapclient.imap_utf7 import encode

from emailsync.connectors.imap import decode_label
from emailsync.mail.source import ImapSource
from emailsync.options import Endpoint, Options


class FakeImap:
    labels = ["\\Inbox", "Projects/Planning", "\\Starred"]

    def ensure_selected(self, _name):
        pass

    def has(self, capability):
        return capability == "X-GM-EXT-1"

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
