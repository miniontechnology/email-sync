import httpx

from emailsync.connectors.jmap import JmapClient
from emailsync.options import Endpoint


def test_download_substitutes_session_url_template():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=b"message bytes")

    client = JmapClient(Endpoint("jmap", url="https://mail.example"))
    client.account_id = "account one"
    client.download_url = "https://mail.example/download/{accountId}/{blobId}?type={type}&name={name}"
    client.http = httpx.Client(transport=httpx.MockTransport(handler))

    assert client.download("blob/one") == b"message bytes"
    request = requests[0]
    assert request.url.raw_path.startswith(b"/download/account%20one/blob%2Fone?")
    assert request.url.params["type"] == "message/rfc822"
    assert request.url.params["name"] == "message.eml"
    client.close()
