import httpx

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


def test_dav_client_update_uses_if_match_and_returns_false_for_stale_etag():
    requests = []
    statuses = iter((204, 412))

    def handler(request):
        requests.append(request)
        return httpx.Response(next(statuses), request=request)

    client = DavClient(Endpoint("carddav", url="https://dav.example.test"))
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    try:
        href = "https://dav.example.test/addressbook/person.vcf"
        assert client.update(href, b"updated card", '"old-etag"') is True
        assert client.update(href, b"stale card", '"old-etag"') is False
        assert [request.headers["If-Match"] for request in requests] == ['"old-etag"', '"old-etag"']
        assert [request.content for request in requests] == [b"updated card", b"stale card"]
    finally:
        client.close()
