from emailsync.connectors.dav import DavClient, _auth_for, _safe_url
from emailsync.options import Endpoint


def test_redirects_strip_userinfo_and_auth_only_on_same_host_secure_upgrade():
    base = "http://dav.example.test:8080/"
    assert _safe_url("https://alice:secret@dav.example.test/dav/") == "https://dav.example.test/dav/"
    assert _auth_for("https://dav.example.test/dav/", base, ("alice", "secret")) == ("alice", "secret")
    assert _auth_for("http://dav.example.test/dav/", "https://dav.example.test/", ("alice", "secret")) is None
    assert _auth_for("https://other.example.test/dav/", base, ("alice", "secret")) is None


def test_dav_client_accepts_endpoint_base_with_user_credentials():
    client = DavClient(
        Endpoint("carddav", url="https://dav.example.test", username="alice", password="secret")
    )
    try:
        assert client.auth == ("alice", "secret")
        assert client.base == "https://dav.example.test/"
    finally:
        client.close()
