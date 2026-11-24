import httpx
import pytest

from emailsync.connectors.dav import DavClient, _auth_for, _safe_url
from emailsync.errors import FatalError, PermanentError, TransientError
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


def test_dav_client_put_returns_created_url_and_false_when_it_exists():
    requests = []
    statuses = iter((201, 204, 412))

    def handler(request):
        requests.append(request)
        return httpx.Response(next(statuses), request=request)

    client = DavClient(Endpoint("carddav", url="https://dav.example.test"))
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    try:
        collection = {"href": "https://dav.example.test/addressbook/"}
        assert client.put(collection, "person-1.vcf", b"card") == (
            "https://dav.example.test/addressbook/person-1.vcf"
        )
        assert client.put(collection, "person-1.vcf", b"card") == (
            "https://dav.example.test/addressbook/person-1.vcf"
        )
        assert client.put(collection, "person-1.vcf", b"card") is False
        assert requests[0].headers["If-None-Match"] == "*"
    finally:
        client.close()


@pytest.mark.parametrize(
    ("status", "error"),
    [(403, PermanentError), (409, PermanentError), (503, TransientError), (507, FatalError)],
)
def test_dav_client_put_classifies_error_statuses(status, error):
    def handler(request):
        return httpx.Response(status, request=request)

    client = DavClient(Endpoint("carddav", url="https://dav.example.test"))
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    try:
        with pytest.raises(error, match=f"HTTP {status}|{status}"):
            client.put({"href": "https://dav.example.test/addressbook/"}, "person.vcf", b"card")
    finally:
        client.close()


def test_dav_client_get_and_conditional_delete_handle_missing_and_stale_objects():
    requests = []
    responses = iter(
        (
            (200, {"ETag": '"etag-1"'}, b"vCard"),
            (404, {}, b""),
            (204, {}, b""),
            (412, {}, b""),
            (404, {}, b""),
        )
    )

    def handler(request):
        requests.append(request)
        status, headers, content = next(responses)
        return httpx.Response(status, headers=headers, content=content, request=request)

    client = DavClient(Endpoint("carddav", url="https://dav.example.test"))
    client.http = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    href = "https://dav.example.test/addressbook/person-1.vcf"
    try:
        assert client.get(href) == ('"etag-1"', "vCard")
        assert client.get(href) is None
        assert client.delete(href, '"etag-1"') is True
        assert client.delete(href, '"etag-1"') is False
        assert client.delete(href, None) is True
        assert requests[2].headers["If-Match"] == '"etag-1"'
        assert requests[3].headers["If-Match"] == '"etag-1"'
        assert "If-Match" not in requests[4].headers
    finally:
        client.close()
