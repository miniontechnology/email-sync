"""Small helpers for splitting and identifying vCard and iCalendar data."""

import hashlib
import re
from collections import defaultdict

from icalendar import Calendar


def _unfold(text: str) -> str:
    return re.sub(r"\r?\n[ \t]", "", text)


def split_vcards(text: str) -> list[str]:
    lines = _unfold(text).splitlines()
    cards = []
    current = []
    active = False
    for line in lines:
        marker = line.strip().upper()
        if marker == "BEGIN:VCARD":
            current = [line]
            active = True
        elif active:
            current.append(line)
            if marker == "END:VCARD":
                cards.append("\r\n".join(current) + "\r\n")
                current = []
                active = False
    return cards


def _properties(card: str):
    for line in _unfold(card).splitlines():
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        yield name.split(";", 1)[0].upper(), value.strip()


def vcard_info(card: str) -> tuple[str | None, str, list[str]]:
    uid = None
    fn = ""
    emails = []
    for name, value in _properties(card):
        if name == "UID" and value:
            uid = value
        elif name == "FN" and not fn:
            fn = value
        elif name == "EMAIL" and value:
            emails.append(value.lower())
    return uid, fn, sorted(set(emails))


def ensure_vcard_uid(card: str) -> tuple[str, str]:
    uid, _fn, _emails = vcard_info(card)
    if uid:
        return uid, card
    normalized = "\n".join(line.strip() for line in _unfold(card).splitlines() if line.strip())
    uid = "email-sync-" + hashlib.sha1(normalized.encode("utf-8", "replace")).hexdigest()[:24]
    match = re.search(r"(?im)^END:VCARD\s*$", card)
    if not match:
        raise ValueError("Invalid vCard: missing END:VCARD")
    updated = card[: match.start()] + f"UID:{uid}\r\n" + card[match.start() :]
    return uid, updated


def contact_fingerprint(fn: str, emails: list[str]) -> str:
    material = "\x1f".join((fn.strip().casefold(), "\x1e".join(sorted(e.casefold() for e in emails))))
    return "contact:" + hashlib.sha256(material.encode("utf-8", "replace")).hexdigest()


def _component_uid(component) -> str | None:
    value = component.get("uid")
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _timezone_ids(components) -> set[str]:
    ids = set()
    for component in components:
        for part in component.walk():
            for _name, prop in part.property_items():
                params = getattr(prop, "params", {})
                tzid = params.get("TZID")
                if tzid:
                    ids.add(str(tzid))
    return ids


def split_ics(text: str | bytes) -> list[tuple[str, bytes]]:
    calendar = Calendar.from_ical(text.encode() if isinstance(text, str) else text)
    groups = defaultdict(list)
    for component in calendar.subcomponents:
        if component.name not in ("VEVENT", "VTODO", "VJOURNAL"):
            continue
        uid = _component_uid(component)
        if not uid:
            material = component.to_ical()
            uid = "email-sync-" + hashlib.sha1(material).hexdigest()[:24]
            component.add("uid", uid)
        groups[uid].append(component)

    zones = {str(zone.get("tzid")): zone for zone in calendar.subcomponents if zone.name == "VTIMEZONE"}
    result = []
    for uid, components in groups.items():
        out = Calendar()
        out.add("version", calendar.get("version", "2.0"))
        out.add("prodid", calendar.get("prodid", "-//email-sync//v1//EN"))
        for tzid in _timezone_ids(components):
            if tzid in zones:
                out.add_component(zones[tzid])
        for component in components:
            out.add_component(component)
        result.append((uid, out.to_ical()))
    return result


def event_fingerprint(summary: str, dtstart) -> str:
    value = (
        dtstart.to_ical().decode("utf-8", "replace") if hasattr(dtstart, "to_ical") else str(dtstart or "")
    )
    material = "\x1f".join((summary.strip().casefold(), value))
    return "event:" + hashlib.sha256(material.encode("utf-8", "replace")).hexdigest()
