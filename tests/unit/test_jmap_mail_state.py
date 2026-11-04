import json

import httpx

from emailsync.connectors.jmap import JmapClient, keywords_to_flags
from emailsync.mail.dest import JmapDest
from emailsync.mail.state import DestCopy, StateChange
from emailsync.options import Endpoint, Options


def test_keywords_to_flags_normalizes_system_and_custom_keywords():
    assert keywords_to_flags({"$seen": True, "$forwarded": True, "Custom": True}) == frozenset(
        {"\\seen", "$forwarded", "custom"}
    )


def test_jmap_state_updates_use_keyword_and_mailbox_patches_and_map_errors():
    seen = []

    def handler(request):
        payload = json.loads(request.content)
        seen.extend(payload["methodCalls"])
        return httpx.Response(
            200,
            json={
                "methodResponses": [
                    [
                        "Email/set",
                        {"notUpdated": {"email-1": {"type": "invalidProperties"}}},
                        "0",
                    ]
                ]
            },
        )

    client = JmapClient(Endpoint("jmap", url="https://jmap.example.test"))
    client.http.close()
    client.http = httpx.Client(transport=httpx.MockTransport(handler))
    client.account_id = "account"
    client.api_url = "https://jmap.example.test/api"
    destination = JmapDest(Endpoint("jmap", url="https://jmap.example.test"), Options())
    destination.client = client
    destination.path_to_id = {"INBOX": "mailbox-inbox", "Work": "mailbox-work"}
    copy = DestCopy("email-1", frozenset({"INBOX"}), frozenset({"\\seen"}))
    change = StateChange(
        "source|1|1",
        [copy],
        [(copy, frozenset({"\\flagged"}), frozenset({"\\seen"}))],
        ["Work"],
        ["INBOX"],
    )
    try:
        errors = destination.apply_state([change], retry=lambda operation: operation())
    finally:
        destination.close()

    assert seen == [
        [
            "Email/set",
            {
                "accountId": "account",
                "update": {
                    "email-1": {
                        "keywords/$flagged": True,
                        "keywords/$seen": None,
                        "mailboxIds/mailbox-work": True,
                        "mailboxIds/mailbox-inbox": None,
                    }
                },
            },
            "0",
        ]
    ]
    assert errors == {"source|1|1": "invalidProperties"}


def test_update_emails_batches_at_most_one_hundred_updates():
    batch_sizes = []

    def handler(request):
        batch_sizes.append(len(json.loads(request.content)["methodCalls"][0][1]["update"]))
        return httpx.Response(
            200,
            json={"methodResponses": [["Email/set", {"updated": {}}, "0"]]},
        )

    client = JmapClient(Endpoint("jmap", url="https://jmap.example.test"))
    client.http.close()
    client.http = httpx.Client(transport=httpx.MockTransport(handler))
    client.account_id = "account"
    client.api_url = "https://jmap.example.test/api"
    try:
        assert client.update_emails({f"id-{i}": {"keywords/$seen": True} for i in range(205)}) == {}
    finally:
        client.close()
    assert batch_sizes == [100, 100, 5]


def test_email_index_requests_keywords_property():
    calls = []

    def handler(request):
        calls.extend(json.loads(request.content)["methodCalls"])
        return httpx.Response(
            200,
            json={
                "methodResponses": [
                    ["Email/query", {"ids": []}, "0"],
                    ["Email/get", {"list": []}, "1"],
                ]
            },
        )

    client = JmapClient(Endpoint("jmap", url="https://jmap.example.test"))
    client.http.close()
    client.http = httpx.Client(transport=httpx.MockTransport(handler))
    client.account_id = "account"
    client.api_url = "https://jmap.example.test/api"
    try:
        list(client.iter_email_index())
    finally:
        client.close()
    assert "keywords" in calls[1][1]["properties"]
