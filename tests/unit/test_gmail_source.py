from imapclient.imap_utf7 import encode

from emailsync.connectors.imap import decode_label
from emailsync.mail.source import ImapSource
from emailsync.options import Endpoint, Options


class FakeImap:
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
                "labels": ["\\Inbox", "Projects/Planning", "\\Starred"],
                "internaldate": None,
                "size": 3,
            }
        }


def test_gmail_uses_all_mail_labels_and_starred_flag():
    source = ImapSource(Endpoint("gmail", host="imap.gmail.com"), Options(), gmail=True)
    source.conn = FakeImap()
    scopes = source.scopes()
    assert [scope.name for scope in scopes] == ["[Gmail]/All Mail"]
    assert source.open_scope(scopes[0]) == (1, [1])
    meta = source.fetch_meta(scopes[0], [1])[1]
    assert meta.targets == ["@inbox", "Projects/Planning"]
    assert meta.flags == ("\\Flagged",)


def test_gmail_optional_spam_trash_and_modified_utf7_labels():
    source = ImapSource(
        Endpoint("gmail", host="imap.gmail.com"), Options(include_spam=True, include_trash=True), gmail=True
    )
    source.conn = FakeImap()
    assert [scope.name for scope in source.scopes()] == ["[Gmail]/All Mail", "[Gmail]/Spam", "[Gmail]/Trash"]
    assert decode_label(encode("Projet été")) == "Projet été"
