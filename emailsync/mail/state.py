"""Planning for source-driven updates to already-copied messages."""

from dataclasses import dataclass

SYNCED_SYSTEM_FLAGS = {"\\seen", "\\flagged", "\\answered", "\\draft"}
_KEYWORD_FORBIDDEN = set('()"{]%*\\ ')
_IMAP_SYSTEM_FLAGS = {
    "\\seen": "\\Seen",
    "\\flagged": "\\Flagged",
    "\\answered": "\\Answered",
    "\\draft": "\\Draft",
}


def normalize_flags(flags) -> frozenset[str]:
    normalized = set()
    for value in flags or ():
        flag = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
        flag = flag.lower()
        if flag in SYNCED_SYSTEM_FLAGS:
            normalized.add(flag)
        elif not flag.startswith("\\") and 1 <= len(flag) <= 255 and not (set(flag) & _KEYWORD_FORBIDDEN):
            normalized.add(flag)
    return frozenset(normalized)


def imap_flag(flag: str) -> str:
    return _IMAP_SYSTEM_FLAGS.get(flag.lower(), flag)


@dataclass(frozen=True)
class DestCopy:
    ref: str
    folders: frozenset[str]
    flags: frozenset[str]


@dataclass
class StateChange:
    source_key: str
    copies: list[DestCopy]
    flag_ops: list[tuple[DestCopy, frozenset, frozenset]]
    add_folders: list[str]
    remove_folders: list[str]

    @property
    def empty(self) -> bool:
        return not self.flag_ops and not self.add_folders and not self.remove_folders


def plan_change(
    source_key,
    copies,
    present_folders: set[str],
    old_flags: frozenset | None,
    new_flags: frozenset,
    old_targets: list[str],
    new_targets: list[str],
    archived: bool,
    remove_stale: bool,
) -> StateChange:
    copies = list(copies)
    present_folders = set(present_folders)
    old_target_set = set(old_targets)
    new_target_set = set(new_targets)
    added_src = list(dict.fromkeys(t for t in new_targets if t not in old_target_set))
    removed_src = list(dict.fromkeys(t for t in old_targets if t not in new_target_set))

    if archived and not remove_stale and present_folders:
        added_src = []
    add_folders = [target for target in added_src if target not in present_folders]
    copied_folders = {folder for copy in copies for folder in copy.folders}
    remove_folders = [target for target in removed_src if target in copied_folders] if remove_stale else []
    if not (present_folders | set(add_folders)) - set(remove_folders):
        remove_folders = []

    normalized_new = normalize_flags(new_flags)
    if old_flags is None:
        plus = normalized_new
        minus = frozenset()
    else:
        normalized_old = normalize_flags(old_flags)
        plus = normalized_new - normalized_old
        minus = normalized_old - normalized_new

    flag_ops = []
    removing = set(remove_folders)
    for copy in copies:
        if copy.folders and copy.folders <= removing:
            continue
        copy_flags = normalize_flags(copy.flags)
        desired = (copy_flags | plus) - minus
        add = desired - copy_flags
        remove = copy_flags - desired
        if add or remove:
            flag_ops.append((copy, frozenset(add), frozenset(remove)))

    return StateChange(source_key, copies, flag_ops, add_folders, remove_folders)
