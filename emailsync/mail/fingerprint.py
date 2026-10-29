"""Message identity: how we recognise "the same message" on two different servers.

Primary key is the Message-ID header. Messages without one fall back to a fingerprint built only
from fields every server preserves (Date, From, To, Subject) so that copies made by other tools,
which may re-encode bodies or change sizes, still match.
"""

import datetime as dt
import email
import email.policy
import hashlib
import re
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime

_MSGID_RE = re.compile(r"<([^<>]+)>")
_WS_RE = re.compile(r"\s+")

HEADER_FIELDS = ("MESSAGE-ID", "DATE", "FROM", "TO", "SUBJECT")


def normalize_message_id(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    m = _MSGID_RE.search(raw)
    value = m.group(1) if m else raw
    value = _WS_RE.sub("", value)
    return value or None


def decode_mime(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def normalize_subject(subject: str | None) -> str:
    return _WS_RE.sub(" ", subject or "").strip()


def addresses(value: str | None) -> list[str]:
    if not value:
        return []
    return sorted({addr.strip().lower() for _, addr in getaddresses([decode_mime(value)]) if addr.strip()})


def parse_date(value: str | None) -> int | None:
    if not value:
        return None
    try:
        d = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.UTC)
    return int(d.timestamp())


def fingerprint(date_ts: int | None, from_addrs: list[str], to_addrs: list[str], subject: str) -> str:
    material = "\x1f".join(
        [
            str(date_ts or ""),
            ",".join(sorted(from_addrs)),
            ",".join(sorted(to_addrs)),
            normalize_subject(subject),
        ]
    )
    return "fp:" + hashlib.sha256(material.encode("utf-8", "replace")).hexdigest()[:32]


def match_key(message_id: str | None, date_ts: int | None, from_addrs, to_addrs, subject: str) -> str:
    mid = normalize_message_id(message_id)
    if mid:
        return "mid:" + mid
    return fingerprint(date_ts, list(from_addrs), list(to_addrs), subject)


def summary_from_headers(header_bytes: bytes) -> dict:
    """Parse raw header bytes (from IMAP FETCH) into the fields used for matching and display."""
    msg = email.message_from_bytes(header_bytes or b"", policy=email.policy.compat32)
    subject = decode_mime(msg.get("Subject"))
    date_ts = parse_date(msg.get("Date"))
    from_addrs = addresses(msg.get("From"))
    to_addrs = addresses(msg.get("To"))
    key = match_key(msg.get("Message-ID"), date_ts, from_addrs, to_addrs, subject)
    return {"key": key, "subject": subject, "from": ",".join(from_addrs), "date": date_ts}


def key_from_jmap(obj: dict) -> str:
    """Same key as summary_from_headers, computed from a JMAP Email object."""
    ids = obj.get("messageId") or []
    date_ts = None
    if obj.get("sentAt"):
        try:
            date_ts = int(dt.datetime.fromisoformat(obj["sentAt"].replace("Z", "+00:00")).timestamp())
        except ValueError:
            date_ts = None
    from_addrs = [a.get("email", "").lower() for a in (obj.get("from") or []) if a.get("email")]
    to_addrs = [a.get("email", "").lower() for a in (obj.get("to") or []) if a.get("email")]
    return match_key(
        ids[0] if ids else None,
        date_ts,
        sorted(set(from_addrs)),
        sorted(set(to_addrs)),
        obj.get("subject") or "",
    )
