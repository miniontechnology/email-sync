# Email Sync

Email Sync is a self-hosted migration tool for mail, contacts, and calendars. It runs as one
Docker container with a small browser UI. Migrations run in the background, independently of
the browser, and can be paused, resumed, and retried.

Supported sources and destinations:

- Mail: generic IMAP, Gmail over IMAP, and JMAP destinations.
- Contacts: CardDAV and vCard files.
- Calendars: CalDAV and iCalendar files.
- Takeout `.zip` archives containing `.vcf` or `.ics` files are supported.

The UI has no login and is intended for a trusted private LAN. Do not expose it directly to the
public internet.

## Quick start

On an x86 Synology NAS with Container Manager / Docker Compose:

1. Create a directory for the project and place the repository's `docker-compose.yml` there.
2. In that directory, create a `.env` file containing a stable secret key:

   ```dotenv
   EMAIL_SYNC_SECRET_KEY=replace-with-a-long-random-value
   ```

   Generate a value with a password manager or `openssl rand -hex 32`. Keep this value safe:
   it is used to encrypt saved endpoint credentials.
3. Start the application:

   ```sh
   docker compose up -d --build
   ```
4. Open `http://<nas-address>:8080`.

The named `email-sync-data` volume stores the SQLite database, encryption key when no key was
configured, and any uploaded Takeout files. Back this volume up before upgrading or moving hosts.
Changing `EMAIL_SYNC_SECRET_KEY` after creating jobs makes their saved credentials unreadable.

## Gmail to Stalwart

1. In Google Account security, enable two-step verification and create an app password. Confirm
   that IMAP access is enabled in Gmail settings. Use the Gmail address and app password in the
   source fields; select the Gmail preset.
2. For mail, configure Stalwart as the JMAP destination. Use the account's JMAP base URL, for
   example `https://mail.example.com`, and the full account name and password (or a JMAP token).
   Allow self-signed TLS only for a trusted test server.
3. For contacts and calendars, Google CardDAV/CalDAV requires OAuth, which this application does
   not implement. Export contacts as vCard and calendars as iCalendar using Google Takeout, then
   upload the `.vcf`, `.ics`, or enclosing `.zip` files as file sources. Configure Stalwart's
   CardDAV and CalDAV endpoints as the destinations. DAV credentials may be left blank to reuse
   credentials from that side's mail endpoint.
4. Run a dry run first. Review folder mappings, selected source scopes, and progress before
   running a real copy.

Gmail's IMAP bandwidth limits can temporarily throttle large migrations. Email Sync honors
server throttling, waits, and continues without counting a throttle wait as a failed item.

## Resume and duplicate handling

Each source item is recorded in a local ledger before and after upload. When a run resumes, it
reconciles unfinished uploads against the destination and only copies missing messages. It also
indexes destination messages before copying, so messages already copied by another tool are
counted and not re-imported. Message matching uses Message-ID where available and a stable
header fingerprint otherwise. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details and
limitations.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `EMAIL_SYNC_DATA_DIR` | `/data` | Persistent database, key, and uploads directory |
| `EMAIL_SYNC_SECRET_KEY` | generated key file | Stable secret used to encrypt stored credentials |
| `EMAIL_SYNC_MAX_CONCURRENT_JOBS` | `4` | Maximum number of jobs running at once |
| `EMAIL_SYNC_AUTO_RESUME` | `true` | Resume queued/running jobs after a process restart |
| `EMAIL_SYNC_LOG_LEVEL` | `INFO` | Application log level |

## Development and tests

Python 3.11 or newer is supported. Install development dependencies and run the unit suite:

```sh
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pytest -q
```

Integration tests require Docker Compose and local service ports listed in
[`tests/integration/README.md`](tests/integration/README.md):

```sh
docker compose -f tests/integration/docker-compose.yml up -d
tests/integration/setup.sh
.venv/bin/pytest -q -m integration
docker compose -f tests/integration/docker-compose.yml down
```
