"""Folder/label mapping between source and destination.

Targets are expressed in a server-neutral form:
  "@inbox", "@sent", "@drafts", "@trash", "@junk", "@archive"  -> a special-use role
  "Work/Projects"                                               -> a path, "/"-separated
The destination connector resolves targets to real folder names (and creates them if needed).
"""

from dataclasses import dataclass, field

from emailsync.options import Options

ROLE_FLAGS = {
    "\\sent": "sent",
    "\\drafts": "drafts",
    "\\trash": "trash",
    "\\junk": "junk",
    "\\archive": "archive",
    "\\all": "all",
    "\\flagged": "flagged",
    "\\important": "important",
}

ROLE_NAMES = {
    "sent": ("sent", "sent items", "sent messages", "sent mail"),
    "drafts": ("drafts", "draft"),
    "trash": ("trash", "deleted items", "deleted messages", "deleted", "bin"),
    "junk": ("junk", "spam", "junk e-mail", "junk email", "junk mail", "bulk mail"),
    "archive": ("archive", "archives"),
}

DEFAULT_ROLE_NAMES = {
    "inbox": "INBOX",
    "sent": "Sent",
    "drafts": "Drafts",
    "trash": "Trash",
    "junk": "Junk",
    "archive": "Archive",
}

MAILBOX_ROLES = tuple(DEFAULT_ROLE_NAMES)

GMAIL_SYSTEM_LABELS = {
    "\\inbox": "@inbox",
    "\\sent": "@sent",
    "\\draft": "@drafts",
    "\\drafts": "@drafts",
    "\\trash": "@trash",
    "\\spam": "@junk",
    "\\junk": "@junk",
}


@dataclass
class Folder:
    name: str
    delimiter: str | None = "/"
    flags: tuple[str, ...] = field(default_factory=tuple)

    @property
    def role(self) -> str | None:
        return folder_role(self.name, self.flags, self.delimiter)

    @property
    def selectable(self) -> bool:
        lowered = {f.lower() for f in self.flags}
        return "\\noselect" not in lowered and "\\nonexistent" not in lowered

    def parts(self) -> list[str]:
        if not self.delimiter:
            return [self.name]
        return [p.replace("/", "_") if self.delimiter != "/" else p for p in self.name.split(self.delimiter)]


def folder_role(name: str, flags=(), delimiter: str | None = "/") -> str | None:
    if name.upper() == "INBOX":
        return "inbox"
    for flag in flags or ():
        role = ROLE_FLAGS.get(str(flag).lower())
        if role:
            return role
    lowered = name.lower()
    parts = lowered.split(delimiter) if delimiter else [lowered]
    if len(parts) == 1 or (len(parts) == 2 and parts[0] in ("inbox", "[gmail]", "[google mail]")):
        leaf = parts[-1]
        for role, names in ROLE_NAMES.items():
            if leaf in names:
                return role
    return None


def map_source_folder(folder: Folder, options: Options) -> str | None:
    """Target for every message in `folder` (generic IMAP source). None means skip the folder."""
    if folder.name in options.exclude_folders:
        return None
    if folder.name in options.folder_map:
        mapped = options.folder_map[folder.name].strip()
        return mapped or None
    role = folder.role
    if role == "junk" and not options.include_spam:
        return None
    if role == "trash" and not options.include_trash:
        return None
    if role in ("all", "flagged", "important"):
        return None  # virtual folders: their messages are already in real folders
    if role in MAILBOX_ROLES:
        return "@" + role
    parts = folder.parts()
    if parts and parts[0].upper() == "INBOX" and len(parts) > 1:
        # Courier/Cyrus style "INBOX.Work" -> "Work"; keeps hierarchy portable between servers.
        parts = parts[1:]
    return "/".join(parts)


def gmail_targets(labels, options: Options) -> tuple[list[str], bool]:
    """Targets for a Gmail message from its X-GM-LABELS. Returns (targets, starred)."""
    targets: list[str] = []
    starred = False
    for label in labels or ():
        lowered = label.lower()
        if lowered == "\\starred":
            starred = True
            continue
        if label in options.exclude_folders:
            continue
        if label in options.folder_map:
            mapped = options.folder_map[label].strip()
            if mapped:
                targets.append(mapped)
            continue
        if lowered == "\\important":
            if options.gmail_important_as_folder:
                targets.append("Important")
            continue
        if lowered in GMAIL_SYSTEM_LABELS:
            targets.append(GMAIL_SYSTEM_LABELS[lowered])
            continue
        if lowered.startswith("\\"):
            continue  # unknown system label
        targets.append(label)
    if not targets and options.archive_target:
        targets.append(options.archive_target)
    seen: set[str] = set()
    unique = [t for t in targets if not (t in seen or seen.add(t))]
    return unique, starred


def gmail_folder_label(folder: Folder) -> str | None:
    """Label name Gmail uses in X-GM-LABELS for a folder shown in LIST (for UI previews)."""
    role = folder.role
    if role == "inbox":
        return "\\Inbox"
    if folder.name.startswith("[Gmail]") or folder.name.startswith("[Google Mail]"):
        return {
            "sent": "\\Sent",
            "drafts": "\\Draft",
            "important": "\\Important",
            "flagged": "\\Starred",
        }.get(role or "")
    return folder.name


def parse_folder_map(text: str) -> dict[str, str]:
    """Parse "source => dest" lines. An empty right-hand side means skip."""
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=>" not in line:
            continue
        src, _, dst = line.partition("=>")
        if src.strip():
            out[src.strip()] = dst.strip()
    return out


def format_folder_map(mapping: dict[str, str]) -> str:
    return "\n".join(f"{k} => {v}" for k, v in mapping.items())
