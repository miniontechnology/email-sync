# Architecture

```
Browser ── polling ──> FastAPI (emailsync.web.app)
                          │
                          ├── saved accounts + OAuth (encrypted secrets / token refresh)
                          │
                          ▼
                   JobManager (emailsync.jobs.manager)  ── one thread per running job
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   MailMigration    DavMigration(contacts)  DavMigration(calendars)
   (emailsync.mail.pipeline)  (emailsync.dav.pipeline)
          │
   source connector ──> coordinator ──> worker pool ──> dest connector
                            │                │
                            └── SQLite ledger (emailsync.db) ──┘
```

## Message identity
`emailsync.mail.fingerprint.match_key`: `mid:<Message-ID>` or, when a message has no Message-ID,
`fp:<sha256(Date, From, To, Subject)>`. Size and body are deliberately not used so copies made by other
tools (which may re-encode) still match.

## De-duplication (in order)
1. **Ledger** (`ledger` table): one row per source item, keyed by `folder|UIDVALIDITY|UID`
   (mail) or `collection|UID` (DAV). Status `uploading → done`, or `skipped` (already in destination),
   or `failed`. Rows store the match key and the resolved destination folders.
2. **Destination index** (`emailsync.mail.index.DestIndex`): built from destination headers at the
   start of every run. Counting semantics: each destination copy satisfies exactly one source message,
   so legitimately duplicated Message-IDs stay correct. Settled ledger rows "consume" their copies first.
   A crash between upload and ledger commit leaves the row in `uploading`; next run the index finds
   the copy and the row becomes `skipped` — nothing is uploaded twice.
3. **DAV**: UID match, then optional fingerprint (FN+emails / SUMMARY+DTSTART), then `If-None-Match: *` on PUT.
   JMAP contacts/calendars can be destinations when the server has the corresponding parse capability.
   JMAP calendars match by UID only and do not use event fingerprints.

## Mail flow
- Coordinator (one source + one dest connection): per scope (folder, or Gmail "All Mail"):
  `SEARCH` UIDs → ledger lookup → fetch headers/flags/labels only for unsettled UIDs → resolve targets →
  `DestIndex.decide` → enqueue batches (≤25 messages / ≤20 MB).
- Workers (N, each with its own source + dest connection): fetch bodies, mark `uploading`, upload,
  mark `done`. Transient errors reconnect + exponential backoff; throttling pauses the whole job;
  auth/quota errors abort the task; anything else fails just that item (retried on the next run).
- Optional verify pass re-indexes the destination and flips any settled item that is missing back to
  `failed`, so the next run re-copies it.

## Gmail
Read `[Gmail]/All Mail` once; `X-GM-LABELS` decide destination folders (`\Inbox`→INBOX, `\Sent`→Sent,
`\Draft`→Drafts, user labels → folders, no label → Archive, `\Starred` → `\Flagged`, `\Important` ignored).
JMAP destinations get one stored copy in several mailboxes; IMAP destinations get one APPEND per folder.
Spam/Trash are separate scopes, excluded by default.

## Jobs
States: `queued, running, paused, cancelled, completed, completed_with_errors, failed, interrupted`.
Pause/cancel stop the job gracefully; resume simply runs it again (the ledger makes that cheap).
On startup, jobs left `running`/`queued` are resumed (`EMAIL_SYNC_AUTO_RESUME=true`, default).
Sync jobs (`sync_interval_minutes > 0`) are re-run by the scheduler after they finish.

## Saved accounts and OAuth
The `accounts` table stores reusable server connection settings separately from job definitions.
Account passwords, tokens, client secrets, and OAuth tokens are encrypted with the same `SecretBox`
as job credentials. Jobs retain only an `account_id`; the manager resolves current account settings
when each run starts, so re-signing-in repairs all jobs that use the account. A process-wide token
manager serializes refreshes per account. If refresh is revoked, the affected task pauses and must
be resumed after the user signs in again.
