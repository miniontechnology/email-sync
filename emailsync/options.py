"""Job configuration: endpoints (where to read/write) and migration options."""

from dataclasses import asdict, dataclass, field, fields
from typing import Any

SECRET_FIELDS = ("password", "token")
KINDS = ("mail", "contacts", "calendars")

MAIL_SOURCE_TYPES = ("imap", "gmail")
MAIL_DEST_TYPES = ("imap", "jmap")
CONTACTS_SOURCE_TYPES = ("carddav", "file")
CALENDARS_SOURCE_TYPES = ("caldav", "file")


@dataclass
class Endpoint:
    type: str
    account_id: int | None = None
    host: str = ""
    port: int = 0
    security: str = "ssl"  # ssl | starttls | none
    username: str = ""
    password: str = ""
    token: str = ""  # JMAP bearer token (alternative to password)
    url: str = ""  # JMAP / DAV base URL
    insecure_tls: bool = False
    files: list[str] = field(default_factory=list)  # uploaded .vcf/.ics/.zip paths
    token_provider = None

    @classmethod
    def from_dict(cls, data: dict | None) -> "Endpoint | None":
        if not data or not data.get("type"):
            return None
        known = {f.name for f in fields(cls)}
        ep = cls(**{k: v for k, v in data.items() if k in known})
        ep.port = int(ep.port or 0)
        return ep

    def to_dict(self) -> dict:
        return asdict(self)

    def public_dict(self) -> dict:
        d = self.to_dict()
        for k in SECRET_FIELDS:
            d.pop(k, None)
        return d

    def describe(self) -> str:
        if self.type in ("imap", "gmail"):
            return f"{self.type.upper()} {self.username}@{self.host}:{self.effective_port()}"
        if self.type == "file":
            return f"{len(self.files)} uploaded file(s)"
        return f"{self.type.upper()} {self.username} @ {self.url}"

    def effective_port(self) -> int:
        if self.port:
            return self.port
        return 993 if self.security == "ssl" else 143


@dataclass
class Options:
    workers: int = 4
    dedup_scope: str = "folder"  # folder | account
    include_spam: bool = False
    include_trash: bool = False
    skip_deleted: bool = True  # skip messages flagged \Deleted on the source
    archive_target: str = "@archive"  # Gmail messages without a user label
    gmail_important_as_folder: bool = False
    folder_map: dict[str, str] = field(default_factory=dict)  # source folder/label -> dest path ("" = skip)
    exclude_folders: list[str] = field(default_factory=list)
    since: str = ""  # YYYY-MM-DD, only messages on/after this date
    dry_run: bool = False
    verify: bool = True
    sync_interval_minutes: int = 0
    dav_match_fingerprint: bool = True

    @classmethod
    def from_dict(cls, data: dict | None) -> "Options":
        data = data or {}
        known = {f.name for f in fields(cls)}
        opts = cls(**{k: v for k, v in data.items() if k in known})
        opts.workers = max(1, min(int(opts.workers), 16))
        opts.sync_interval_minutes = max(0, int(opts.sync_interval_minutes or 0))
        if opts.dedup_scope not in ("folder", "account"):
            opts.dedup_scope = "folder"
        return opts

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class JobConfig:
    name: str
    endpoints: dict[str, dict[str, Endpoint | None]]  # kind -> {"source": ep, "dest": ep}
    options: Options

    @classmethod
    def from_dict(cls, data: dict, secrets: dict | None = None) -> "JobConfig":
        secrets = secrets or {}
        endpoints: dict[str, dict[str, Endpoint | None]] = {}
        for kind in KINDS:
            pair = (data.get("endpoints") or {}).get(kind) or {}
            endpoints[kind] = {}
            for side in ("source", "dest"):
                raw = dict(pair.get(side) or {})
                raw.update(secrets.get(f"{kind}.{side}", {}))
                endpoints[kind][side] = Endpoint.from_dict(raw)
        return cls(
            name=data.get("name", ""), endpoints=endpoints, options=Options.from_dict(data.get("options"))
        )

    def enabled_kinds(self) -> list[str]:
        return [k for k in KINDS if self.endpoints[k]["source"] and self.endpoints[k]["dest"]]

    def split(self) -> tuple[dict, dict]:
        """Return (public config, secrets) so credentials can be stored encrypted."""
        public: dict[str, Any] = {"name": self.name, "options": self.options.to_dict(), "endpoints": {}}
        secrets: dict[str, dict] = {}
        for kind, pair in self.endpoints.items():
            public["endpoints"][kind] = {}
            for side, ep in pair.items():
                if ep is None:
                    public["endpoints"][kind][side] = None
                    continue
                public["endpoints"][kind][side] = ep.public_dict()
                secret = {k: getattr(ep, k) for k in SECRET_FIELDS if getattr(ep, k)}
                if secret:
                    secrets[f"{kind}.{side}"] = secret
        return public, secrets

    def validate(self) -> list[str]:
        errors = []
        if not self.name.strip():
            errors.append("Name is required.")
        if not self.enabled_kinds():
            errors.append(
                "Configure a source and a destination for at least one of mail, contacts or calendars."
            )
        allowed = {
            "mail": (MAIL_SOURCE_TYPES, MAIL_DEST_TYPES),
            "contacts": (CONTACTS_SOURCE_TYPES, ("carddav", "jmap")),
            "calendars": (CALENDARS_SOURCE_TYPES, ("caldav", "jmap")),
        }
        for kind in KINDS:
            for side, idx in (("source", 0), ("dest", 1)):
                ep = self.endpoints[kind][side]
                if ep is None:
                    continue
                if ep.type not in allowed[kind][idx]:
                    errors.append(f"{kind} {side}: unsupported type {ep.type!r}.")
                    continue
                label = f"{kind.capitalize()} {side}"
                if not ep.account_id:
                    if ep.type in ("imap", "gmail") and not ep.host:
                        errors.append(f"{label}: host is required.")
                    if ep.type in ("jmap", "carddav", "caldav") and not ep.url:
                        errors.append(f"{label}: URL is required.")
                if ep.type == "file" and not ep.files:
                    errors.append(f"{label}: upload at least one file.")
                if ep.type != "file" and not ep.username and not ep.token and not ep.account_id:
                    errors.append(f"{label}: username is required.")
        if self.options.since:
            import datetime as dt

            try:
                dt.date.fromisoformat(self.options.since)
            except ValueError:
                errors.append("'Since' must be a date like 2020-01-31.")
        return errors
