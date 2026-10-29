import pytest

from emailsync.connectors.jmap import (
    CALENDARS,
    CALENDARS_PARSE,
    CONTACTS,
    CONTACTS_PARSE,
    CORE,
    JmapClient,
)
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.vobjects import contact_fingerprint
from emailsync.errors import PermanentError
from emailsync.options import Endpoint, JobConfig, Options


class FakeResponse:
    status_code = 200

    def __init__(self, session):
        self.session = session

    def raise_for_status(self):
        return None

    def json(self):
        return self.session


class FakeHTTP:
    def __init__(self, session):
        self.session = session

    def get(self, _url):
        return FakeResponse(self.session)

    def close(self):
        return None


class FakeJmapClient:
    def __init__(self, endpoint, responses=None, session=None):
        self.endpoint = endpoint
        self.responses = list(responses or [])
        self.session = session or {
            "primaryAccounts": {CONTACTS: "contacts-account", CALENDARS: "calendar-account"},
            "capabilities": {CONTACTS_PARSE: {}, CALENDARS_PARSE: {}},
        }
        self.account_id = ""
        self.max_objects_in_get = 2
        self.calls = []
        self.uploads = []

    def connect(self, capability):
        self.capability = capability
        self.account_id = self.session.get("primaryAccounts", {}).get(capability, "")
        if not self.account_id:
            label = "contacts" if capability == CONTACTS else "calendars"
            raise PermanentError(f"JMAP server does not offer {label} for this account")
        return self

    def call(self, calls, using):
        self.calls.append((calls, using))
        return self.responses.pop(0)

    def upload(self, data, content_type):
        self.uploads.append((data, content_type))
        return "blob-1"

    def close(self):
        return None


def _destination(monkeypatch, kind, responses=(), session=None):
    responses = list(responses)
    collection_name = "AddressBook" if kind == "contacts" else "Calendar"
    get_response = [
        [
            f"{collection_name}/get",
            {
                "list": [
                    {"id": "default", "name": "Default", "isDefault": True},
                ]
            },
            "0",
        ]
    ]
    if not responses or responses[0][0][0] != f"{collection_name}/get":
        responses.insert(0, get_response)
    client = FakeJmapClient(Endpoint("jmap", url="https://jmap.example"), responses, session)
    monkeypatch.setattr("emailsync.dav.jmap_dest.JmapClient", lambda _endpoint: client)
    return JmapDavDestination(Endpoint("jmap", url="https://jmap.example"), kind), client


@pytest.mark.parametrize(
    ("kind", "capability", "account_id", "collection_name"),
    [
        ("contacts", CONTACTS, "contacts-account", "AddressBook"),
        ("calendars", CALENDARS, "calendar-account", "Calendar"),
    ],
)
def test_connect_selects_kind_account_and_places_default_first(
    monkeypatch, kind, capability, account_id, collection_name
):
    collection_rows = [
        {"id": "non-default", "name": "Other", "isDefault": False},
        {"id": "default", "name": "Default", "isDefault": True},
    ]
    response = [[f"{collection_name}/get", {"list": collection_rows}, "0"]]
    destination, client = _destination(monkeypatch, kind, [response])

    assert destination.connect() is destination
    assert client.capability == capability
    assert client.account_id == account_id
    assert [item["id"] for item in destination.collections] == ["default", "non-default"]
    assert destination.home == ""
    assert client.calls[0][1] == (CORE, capability, CONTACTS_PARSE if kind == "contacts" else CALENDARS_PARSE)


def test_jmap_client_uses_primary_account_for_requested_capability(monkeypatch):
    session = {
        "primaryAccounts": {CONTACTS: "address-book-account", CALENDARS: "calendar-account"},
        "apiUrl": "https://internal.example/api",
        "uploadUrl": "https://internal.example/upload/{accountId}",
    }
    monkeypatch.setattr("emailsync.connectors.jmap.httpx.Client", lambda **_kwargs: FakeHTTP(session))
    client = JmapClient(Endpoint("jmap", url="https://mail.example"))
    assert client.connect(CONTACTS).account_id == "address-book-account"
    assert client.connect(CALENDARS).account_id == "calendar-account"
    client.close()


def test_missing_jmap_capability_raises_permanent_error(monkeypatch):
    session = {
        "primaryAccounts": {CONTACTS: "contacts-account"},
        "apiUrl": "https://internal.example/api",
        "uploadUrl": "https://internal.example/upload/{accountId}",
    }
    monkeypatch.setattr("emailsync.connectors.jmap.httpx.Client", lambda **_kwargs: FakeHTTP(session))
    with pytest.raises(PermanentError, match="JMAP server does not offer calendars for this account"):
        JmapClient(Endpoint("jmap", url="https://mail.example")).connect(CALENDARS)


def test_missing_parse_capability_raises_permanent_error(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "contacts",
        session={
            "primaryAccounts": {CONTACTS: "contacts-account"},
            "capabilities": {},
        },
    )
    with pytest.raises(
        PermanentError,
        match=r"JMAP server cannot import vCard/iCalendar data \(missing "
        + CONTACTS_PARSE
        + r"\); use CardDAV/CalDAV instead",
    ):
        destination.connect()


def test_contact_put_strips_nulls_sets_address_book_and_returns_true(monkeypatch):
    parsed_card = {
        "id": None,
        "uid": "contact-1",
        "name": {"full": "Person"},
        "emails": {},
        "isFlagged": None,
    }
    destination, client = _destination(
        monkeypatch,
        "contacts",
        [
            [["ContactCard/parse", {"parsed": {"blob-1": parsed_card}}, "p"]],
            [["ContactCard/set", {"created": {"c0": {"id": "created-1"}}}, "s"]],
        ],
    )
    destination.connect()
    assert destination.put({"href": "book-1", "id": "book-1"}, "person.vcf", b"VCARD") is True
    assert client.uploads == [(b"VCARD", "text/vcard")]
    set_call = client.calls[-1][0][0][1]
    object_data = set_call["create"]["c0"]
    assert object_data == {
        "uid": "contact-1",
        "name": {"full": "Person"},
        "emails": {},
        "addressBookIds": {"book-1": True},
    }
    assert client.calls[-1][1] == (CORE, CONTACTS, CONTACTS_PARSE)


def test_duplicate_uid_returns_false(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "contacts",
        [
            [["ContactCard/parse", {"parsed": {"blob-1": {"uid": "same"}}}, "p"]],
            [
                [
                    "ContactCard/set",
                    {
                        "notCreated": {
                            "c0": {
                                "type": "invalidProperties",
                                "description": "Contact with UID same already exists.",
                                "properties": ["uid"],
                            }
                        }
                    },
                    "s",
                ]
            ],
        ],
    )
    destination.connect()
    assert destination.put({"href": "book-1"}, "same.vcf", b"VCARD") is False


def test_other_not_created_response_raises_permanent_error(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "contacts",
        [
            [["ContactCard/parse", {"parsed": {"blob-1": {"uid": "contact-1"}}}, "p"]],
            [
                [
                    "ContactCard/set",
                    {"notCreated": {"c0": {"type": "invalidProperties", "description": "bad address"}}},
                    "s",
                ]
            ],
        ],
    )
    destination.connect()
    with pytest.raises(PermanentError, match="invalidProperties bad address"):
        destination.put({"href": "book-1"}, "person.vcf", b"VCARD")


def test_partial_set_failure_raises_even_when_an_object_was_created(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "calendars",
        [
            [["CalendarEvent/parse", {"parsed": {"blob-1": [{"uid": "event-1"}, {"uid": "event-2"}]}}, "p"]],
            [
                [
                    "CalendarEvent/set",
                    {
                        "created": {"c0": {"id": "created-1"}},
                        "notCreated": {"c1": {"type": "invalidProperties", "description": "invalid event"}},
                    },
                    "s",
                ]
            ],
        ],
    )
    destination.connect()
    with pytest.raises(PermanentError, match="invalidProperties invalid event"):
        destination.put({"href": "calendar-1"}, "events.ics", b"ICAL")


def test_not_parsable_response_raises_permanent_error(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "contacts",
        [
            [["ContactCard/parse", {"parsed": {}, "notParsable": {"blob-1": {}}}, "p"]],
        ],
    )
    destination.connect()
    with pytest.raises(PermanentError, match="Destination could not parse this contact/event"):
        destination.put({"href": "book-1"}, "person.vcf", b"bad")


def test_calendar_put_disables_scheduling_and_sets_calendar(monkeypatch):
    event = {"id": None, "uid": "event-1", "title": "Meeting", "isDraft": None}
    destination, client = _destination(
        monkeypatch,
        "calendars",
        [
            [["CalendarEvent/parse", {"parsed": {"blob-1": [event]}}, "p"]],
            [["CalendarEvent/set", {"created": {"c0": {"id": "created-1"}}}, "s"]],
        ],
    )
    destination.connect()
    assert destination.put({"href": "calendar-1"}, "event.ics", b"ICAL") is True
    args = client.calls[-1][0][0][1]
    assert args["sendSchedulingMessages"] is False
    assert args["create"]["c0"]["calendarIds"] == {"calendar-1": True}
    assert "id" not in args["create"]["c0"] and "isDraft" not in args["create"]["c0"]
    assert client.calls[-1][1] == (CORE, CALENDARS, CALENDARS_PARSE)


def test_empty_calendar_parse_result_raises_permanent_error(monkeypatch):
    destination, _client = _destination(
        monkeypatch,
        "calendars",
        [[["CalendarEvent/parse", {"parsed": {"blob-1": []}}, "p"]]],
    )
    destination.connect()
    with pytest.raises(PermanentError, match="No VEVENT in item; JMAP calendars only import events"):
        destination.put({"href": "calendar-1"}, "empty.ics", b"VCALENDAR")


def test_items_pages_and_computes_contact_fingerprint(monkeypatch):
    page_one = [
        [
            ["ContactCard/query", {"ids": ["a", "b"]}, "q"],
            [
                "ContactCard/get",
                {
                    "list": [
                        {
                            "uid": "person-1",
                            "name": {"full": "Person"},
                            "emails": {"a": {"address": "p@example.org"}},
                        },
                        {"name": {"full": "No UID"}, "emails": {}},
                    ]
                },
                "g",
            ],
        ],
        [
            ["ContactCard/query", {"ids": ["c"]}, "q"],
            [
                "ContactCard/get",
                {
                    "list": [
                        {"uid": "person-2", "name": {"full": "Second"}, "emails": []},
                    ]
                },
                "g",
            ],
        ],
    ]
    destination, client = _destination(monkeypatch, "contacts", page_one)
    destination.connect()
    items = destination.items({"href": "book-1", "id": "book-1"})
    assert [(item.uid, item.data) for item in items] == [
        ("person-1", b""),
        ("person-2", b""),
    ]
    assert items[0].fingerprint == contact_fingerprint("Person", ["p@example.org"])
    assert [entry[0][0][1]["position"] for entry in client.calls[1:]] == [0, 2]
    assert all(entry[0][0][1]["filter"] == {"inAddressBook": "book-1"} for entry in client.calls[1:])
    assert all(entry[0][0][1]["limit"] == client.max_objects_in_get for entry in client.calls[1:])


def test_create_collection_appends_and_returns_new_address_book(monkeypatch):
    destination, client = _destination(
        monkeypatch,
        "contacts",
        [[["AddressBook/set", {"created": {"c": {"id": "book-2", "name": "Friends"}}}, "0"]]],
    )
    destination.connect()
    collection = destination.create_collection("Friends")
    assert collection == {"href": "book-2", "id": "book-2", "displayname": "Friends"}
    assert destination.collections[-1] == collection
    assert client.calls[-1][1] == (CORE, CONTACTS, CONTACTS_PARSE)


@pytest.mark.parametrize(
    ("kind", "source_type", "dest_type"),
    [("contacts", "carddav", "jmap"), ("calendars", "caldav", "jmap")],
)
def test_options_accept_jmap_destinations(kind, source_type, dest_type):
    endpoints = {name: {"source": None, "dest": None} for name in ("mail", "contacts", "calendars")}
    endpoints[kind] = {
        "source": Endpoint(source_type, url="https://source.example", username="alice"),
        "dest": Endpoint(dest_type, url="https://destination.example", username="alice"),
    }
    config = JobConfig("JMAP destination", endpoints, Options())
    assert config.validate() == []


@pytest.mark.parametrize("kind", ("contacts", "calendars"))
def test_options_reject_jmap_sources(kind):
    endpoints = {name: {"source": None, "dest": None} for name in ("mail", "contacts", "calendars")}
    endpoints[kind] = {
        "source": Endpoint("jmap", url="https://source.example", username="alice"),
        "dest": Endpoint("jmap", url="https://destination.example", username="alice"),
    }
    errors = JobConfig("JMAP source", endpoints, Options()).validate()
    assert f"{kind} source: unsupported type 'jmap'." in errors
