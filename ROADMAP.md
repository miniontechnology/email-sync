# Roadmap

Things deliberately left out of v1 so the core migration path could be made reliable first.
Roughly in priority order; open an issue/PR to reprioritise.

## Sources & destinations
- **Google OAuth 2.0** sign-in (for Gmail IMAP without app passwords, and for Google CardDAV/CalDAV so
  contacts/calendars can be read live instead of from a Takeout export). Requires a user-supplied
  Google Cloud OAuth client.
- **JMAP Contacts / Calendars** (RFC 9610 / draft-ietf-jmap-calendars) as an alternative to CardDAV/CalDAV.
- Gmail mbox import from Google Takeout (for accounts where IMAP is disabled or throttled).
- Microsoft 365 / Exchange (Graph or EWS) and Outlook.com OAuth.
- Sieve filters / Gmail filters export, signatures, vacation responder, other account settings.

## Sync behaviour
- **Two-way sync.**
- **Propagating deletions** (source → destination), with a safety threshold.
- Propagating flag / label changes on messages that were already copied (currently only new messages
  are picked up by repeat syncs).
- Updating contacts/events that changed on the source after they were copied (currently "skip existing").
- CONDSTORE/QRESYNC-based change detection to make large incremental syncs cheaper.

## Performance
- IMAP destination: `APPEND` once + server-side `COPY`/`MULTIAPPEND` for messages with several
  labels (currently appended once per target folder).
- JMAP destination: batching several `Email/import` calls per request.
- Streaming very large messages to disk instead of memory.

## App
- Authentication / multi-user access control for the web UI.
- Editing an existing migration (today: clone it and change the copy).
- Per-folder selection checkboxes in the UI (today: exclude list + mapping text box).
- Notifications (email/webhook) on completion or failure.
- Export of the ledger / migration report as CSV.
- Prometheus metrics endpoint.
