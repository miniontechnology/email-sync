from icalendar import Calendar

from emailsync.dav.vobjects import ensure_vcard_uid, split_ics, split_vcards, vcard_info


def test_split_vcards_unfolds_and_parses_properties():
    cards = split_vcards(
        "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Example Person\r\n"
        "EMAIL;TYPE=HOME:person@example.com\r\nNOTE:long\r\n folded\r\nEND:VCARD\r\n"
    )
    assert len(cards) == 1
    assert "NOTE:longfolded" in cards[0]
    assert vcard_info(cards[0]) == (None, "Example Person", ["person@example.com"])


def test_generated_vcard_uid_is_deterministic():
    card = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Example\r\nEND:VCARD\r\n"
    uid, updated = ensure_vcard_uid(card)
    assert uid.startswith("email-sync-")
    assert ensure_vcard_uid(card) == (uid, updated)
    assert vcard_info(updated)[0] == uid


def test_split_ics_groups_recurrence_and_keeps_only_referenced_timezone():
    text = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//tests//EN
BEGIN:VTIMEZONE
TZID:Europe/Paris
BEGIN:STANDARD
DTSTART:19701025T030000
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
TZNAME:CET
END:STANDARD
END:VTIMEZONE
BEGIN:VTIMEZONE
TZID:America/New_York
BEGIN:STANDARD
DTSTART:19701101T020000
TZOFFSETFROM:-0400
TZOFFSETTO:-0500
TZNAME:EST
END:STANDARD
END:VTIMEZONE
BEGIN:VEVENT
UID:series-1
SUMMARY:Planning
DTSTART;TZID=Europe/Paris:20240101T100000
END:VEVENT
BEGIN:VEVENT
UID:series-1
RECURRENCE-ID;TZID=Europe/Paris:20240108T100000
SUMMARY:Planning moved
DTSTART;TZID=Europe/Paris:20240108T110000
END:VEVENT
BEGIN:VEVENT
SUMMARY:Generated
DTSTART:20240201T100000Z
END:VEVENT
END:VCALENDAR
"""
    objects = split_ics(text)
    assert len(objects) == 2
    groups = {uid: Calendar.from_ical(data) for uid, data in objects}
    series = groups["series-1"]
    assert len([c for c in series.subcomponents if c.name == "VEVENT"]) == 2
    zones = [c for c in series.subcomponents if c.name == "VTIMEZONE"]
    assert [str(c.get("tzid")) for c in zones] == ["Europe/Paris"]
    generated_uid, generated = next((uid, cal) for uid, cal in groups.items() if uid != "series-1")
    assert generated_uid.startswith("email-sync-")
    assert str(next(c for c in generated.subcomponents if c.name == "VEVENT").get("uid")) == generated_uid
