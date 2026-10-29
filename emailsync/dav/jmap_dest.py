"""JMAP destination adapter for contacts and calendar events."""

from emailsync.connectors.jmap import (
    CALENDARS,
    CALENDARS_PARSE,
    CONTACTS,
    CONTACTS_PARSE,
    CORE,
    JmapClient,
)
from emailsync.dav.source import DavItem
from emailsync.dav.vobjects import contact_fingerprint
from emailsync.errors import PermanentError


class JmapDavDestination:
    def __init__(self, endpoint, kind):
        if kind not in ("contacts", "calendars"):
            raise ValueError("kind must be contacts or calendars")
        self.endpoint = endpoint
        self.kind = kind
        self.capability = CONTACTS if kind == "contacts" else CALENDARS
        self.parse_capability = CONTACTS_PARSE if kind == "contacts" else CALENDARS_PARSE
        self.object_name = "ContactCard" if kind == "contacts" else "CalendarEvent"
        self.collection_name = "AddressBook" if kind == "contacts" else "Calendar"
        self.client = None
        self.home = ""
        self.collections = []

    def _using(self):
        return (CORE, self.capability, self.parse_capability)

    def connect(self):
        self.client = JmapClient(self.endpoint).connect(self.capability)
        if self.parse_capability not in self.client.session.get("capabilities", {}):
            raise PermanentError(
                f"JMAP server cannot import vCard/iCalendar data (missing {self.parse_capability}); "
                "use CardDAV/CalDAV instead"
            )
        response = self.client.call(
            [
                [
                    f"{self.collection_name}/get",
                    {
                        "accountId": self.client.account_id,
                        "ids": None,
                        "properties": ["id", "name", "isDefault"],
                    },
                    "0",
                ]
            ],
            using=self._using(),
        )
        rows = response[0][1].get("list", [])
        rows.sort(key=lambda row: not row.get("isDefault", False))
        self.collections = [
            {
                "href": row["id"],
                "id": row["id"],
                "displayname": row.get("name") or row["id"],
            }
            for row in rows
        ]
        self.home = ""
        return self

    def reconnect(self):
        self.close()
        return self.connect()

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

    def create_collection(self, name):
        creation_id = "c"
        response = self.client.call(
            [
                [
                    f"{self.collection_name}/set",
                    {
                        "accountId": self.client.account_id,
                        "create": {creation_id: {"name": name}},
                    },
                    "0",
                ]
            ],
            using=self._using(),
        )
        args = response[0][1]
        created = (args.get("created") or {}).get(creation_id)
        if not created:
            error = (args.get("notCreated") or {}).get(creation_id, {})
            raise PermanentError(
                f"Could not create {self.collection_name.lower()} {name!r}: "
                f"{error.get('type')} {error.get('description', '')}".strip()
            )
        collection = {
            "href": created["id"],
            "id": created["id"],
            "displayname": created.get("name") or name,
        }
        self.collections.append(collection)
        return collection

    def items(self, collection):
        collection_id = collection.get("id") or collection["href"]
        query_filter = (
            {"inAddressBook": collection_id} if self.kind == "contacts" else {"inCalendar": collection_id}
        )
        properties = ["id", "uid", "name", "emails"] if self.kind == "contacts" else ["id", "uid"]
        page_size = self.client.max_objects_in_get
        position = 0
        items = []
        while True:
            response = self.client.call(
                [
                    [
                        f"{self.object_name}/query",
                        {
                            "accountId": self.client.account_id,
                            "filter": query_filter,
                            "position": position,
                            "limit": page_size,
                        },
                        "q",
                    ],
                    [
                        f"{self.object_name}/get",
                        {
                            "accountId": self.client.account_id,
                            "properties": properties,
                            "#ids": {
                                "resultOf": "q",
                                "name": f"{self.object_name}/query",
                                "path": "/ids",
                            },
                        },
                        "g",
                    ],
                ],
                using=self._using(),
            )
            ids = response[0][1].get("ids", [])
            for row in response[1][1].get("list", []):
                uid = row.get("uid")
                if not uid:
                    continue
                fingerprint = ""
                if self.kind == "contacts":
                    name = row.get("name") or {}
                    full_name = name.get("full", "") if isinstance(name, dict) else ""
                    emails = row.get("emails") or {}
                    values = emails.values() if isinstance(emails, dict) else emails
                    addresses = [
                        value.get("address", "") if isinstance(value, dict) else str(value)
                        for value in values
                    ]
                    fingerprint = contact_fingerprint(full_name, addresses)
                items.append(DavItem(str(uid), b"", fingerprint))
            if len(ids) < page_size:
                return items
            position += len(ids)

    def put(self, collection, filename, data):
        content_type = "text/vcard" if self.kind == "contacts" else "text/calendar"
        blob_id = self.client.upload(data, content_type)
        parsed_response = self.client.call(
            [
                [
                    f"{self.object_name}/parse",
                    {"accountId": self.client.account_id, "blobIds": [blob_id]},
                    "p",
                ]
            ],
            using=self._using(),
        )
        parsed = parsed_response[0][1].get("parsed", {}).get(blob_id)
        if parsed is None or blob_id in (parsed_response[0][1].get("notParsable") or {}):
            raise PermanentError("Destination could not parse this contact/event")
        if self.kind == "contacts":
            parsed_objects = [parsed] if isinstance(parsed, dict) else []
        else:
            parsed_objects = parsed if isinstance(parsed, list) else []
            if not parsed_objects:
                raise PermanentError("No VEVENT in item; JMAP calendars only import events")
        collection_id = collection.get("id") or collection["href"]
        create = {}
        for index, value in enumerate(parsed_objects):
            object_data = {key: item for key, item in value.items() if key != "id" and item is not None}
            object_data["addressBookIds" if self.kind == "contacts" else "calendarIds"] = {
                collection_id: True
            }
            create[f"c{index}"] = object_data
        method_args = {"accountId": self.client.account_id, "create": create}
        if self.kind == "calendars":
            method_args["sendSchedulingMessages"] = False
        response = self.client.call(
            [[f"{self.object_name}/set", method_args, "s"]],
            using=self._using(),
        )
        args = response[0][1]
        errors = list((args.get("notCreated") or {}).values())
        nonduplicate_errors = [
            error
            for error in errors
            if not (
                error.get("type") == "alreadyExists"
                or (
                    error.get("type") == "invalidProperties"
                    and "already exists" in error.get("description", "").casefold()
                )
            )
        ]
        if nonduplicate_errors:
            error = nonduplicate_errors[0]
            raise PermanentError(
                f"{self.object_name}/set rejected: {error.get('type')} {error.get('description', '')}".strip()
            )
        if args.get("created"):
            return True
        if errors:
            return False
        raise PermanentError(f"{self.object_name}/set rejected: missing created result")
