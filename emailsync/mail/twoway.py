"""Three-way reconciliation for opt-in two-way mail sync."""

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from emailsync.errors import PermanentError
from emailsync.mail.folders import gmail_folder_label, gmail_real_targets, map_source_folder
from emailsync.mail.index import DestIndex
from emailsync.mail.state import DestCopy, StateChange, normalize_flags


@dataclass
class SrcEntry:
    locations: list[tuple[str, int]] = field(default_factory=list)
    folders: set[str] = field(default_factory=set)
    flags: frozenset[str] = frozenset()
    folder_locations: dict[str, list[tuple[str, int]]] = field(default_factory=lambda: defaultdict(list))
    ambiguous: bool = False
    trash: bool = False


@dataclass
class DstEntry:
    copies: list[DestCopy] = field(default_factory=list)
    folders: set[str] = field(default_factory=set)
    flags: frozenset[str] = frozenset()
    normalize_copy_flags: set[str] = field(default_factory=set)
    mixed_flags: set[str] = field(default_factory=set)
    changed_copy_flags: set[str] = field(default_factory=set)
    ambiguous: bool = False
    trash_copies: list[DestCopy] = field(default_factory=list)


class TwoWayReconciler:
    def __init__(self, store, job_id, source, dest, options, retry, progress, log, failed):
        self.store = store
        self.job_id = job_id
        self.source = source
        self.dest = dest
        self.options = options
        self.retry = retry
        self.progress = progress
        self.log = log
        self.failed = failed
        self.ambiguous = 0
        self.warning_count = 0
        self.unscanned_copybacks = 0
        self.unsearchable_copybacks = 0

    def _resolve(self, target):
        if not target:
            return None
        name, _ = self.retry(lambda: self.dest.resolve(target, create=False), self.dest)
        return name

    def _source_snapshot(self, pairs):
        rows = {
            row["source_key"]: row
            for row in self.store.ledger_items(self.job_id, "mail", limit=None)
            if row["status"] in ("done", "skipped")
        }
        entries: dict[str, SrcEntry] = {}
        scope_counts = {}
        scope_targets = {}
        for scope in self.retry(self.source.scopes, self.source) or []:
            validity, uids = self.retry(lambda scope=scope: self.source.open_scope(scope), self.source)
            scope_counts[scope.name] = len(uids)
            if scope.gmail_labels:
                scope_targets[scope.name] = set()
            else:
                scope_targets[scope.name] = {
                    self._resolve(scope.target) if scope.target else self._resolve(scope.name)
                } - {None}
            keys = {uid: rows.get(f"{scope.name}|{validity}|{uid}") for uid in uids}
            missing = [uid for uid, row in keys.items() if not row or not row.get("match_key")]
            metadata = (
                self.retry(
                    lambda scope=scope, missing=missing: self.source.fetch_meta(scope, missing), self.source
                )
                or {}
                if missing
                else {}
            )
            states = (
                self.retry(lambda scope=scope, uids=uids: self.source.fetch_state(scope, uids), self.source)
                or {}
            )
            for uid in uids:
                row = keys.get(uid)
                meta = metadata.get(uid)
                key = (row.get("match_key") if row else None) or (meta.key if meta else None)
                state = states.get(uid)
                if not key or (not state and not meta):
                    continue
                targets = list(state.targets if state else meta.targets)
                flags = normalize_flags(state.flags if state else meta.flags)
                is_trash = scope.target == "@trash" or "@trash" in targets
                if scope.gmail_labels:
                    if "@trash" in targets:
                        targets = [target for target in targets if target != "@trash"]
                    resolved = {self._resolve(target) for target in targets if target != "@archive"} - {None}
                    if "@archive" in targets and not resolved:
                        resolved.add(self._resolve(self.options.archive_target))
                else:
                    resolved = {self._resolve(target) for target in targets} - {None}
                entry = entries.setdefault(key, SrcEntry())
                location = (scope.name, uid)
                entry.locations.append(location)
                entry.trash = entry.trash or is_trash
                if entry.locations[:-1] and any(
                    old_scope == scope.name for old_scope, _ in entry.locations[:-1]
                ):
                    entry.ambiguous = True
                entry.flags = normalize_flags((*entry.flags, *flags))
                if not is_trash:
                    entry.folders.update(resolved)
                for folder in resolved:
                    entry.folder_locations[folder].append(location)
                scope_targets[scope.name].update(resolved)
        source_counts = Counter()
        for entry in entries.values():
            for folder in entry.folders:
                source_counts[folder] += 1
        empty = bool(pairs and not scope_counts)
        for scope, count in scope_counts.items():
            if (
                count == 0
                and pairs
                and (
                    self.source.gmail
                    or any(
                        set(pair.get("src_folders") or ()) & scope_targets[scope] for pair in pairs.values()
                    )
                )
            ):
                empty = True
                self.log(f"Source folder {scope} looks empty; skipped deletions this run", "warning")
        return entries, source_counts, empty, scope_counts, scope_targets

    def _destination_snapshot(self, pairs):
        index = DestIndex(scope="account")
        self.retry(lambda: self.dest.index_all(index), self.dest)
        trash, _ = self.retry(lambda: self.dest.resolve("@trash", create=False), self.dest)
        entries: dict[str, DstEntry] = {}
        counts = Counter()
        for key, copies in index.entries().items():
            entry = DstEntry()
            pair = pairs.get(key)
            for copy in copies:
                folders = set(copy.folders)
                is_trash = bool(trash and trash in folders)
                if is_trash:
                    entry.trash_copies.append(copy)
                    folders.discard(trash)
                entry.folders.update(folders)
                if folders:
                    for folder in folders:
                        counts[folder] += 1
                    entry.copies.append(DestCopy(copy.ref, frozenset(folders), normalize_flags(copy.flags)))
            copy_flags = [copy.flags for copy in entry.copies]
            flags = set().union(*copy_flags) if copy_flags else set()
            baseline_flags = normalize_flags(pair.get("dst_flags") or ()) if pair else None
            baseline_mixed = normalize_flags(pair.get("dst_mixed") or ()) if pair else frozenset()
            if baseline_flags is not None:
                flags.update(baseline_flags)
            selected_flags = set()
            for flag in flags:
                values = [flag in flags_for_copy for flags_for_copy in copy_flags]
                if not values:
                    continue
                if flag in baseline_mixed:
                    if all(value == values[0] for value in values):
                        selected = values[0]
                        entry.changed_copy_flags.add(flag)
                    else:
                        selected = flag in baseline_flags
                        entry.mixed_flags.add(flag)
                elif all(value == values[0] for value in values):
                    selected = values[0]
                else:
                    entry.mixed_flags.add(flag)
                    entry.normalize_copy_flags.add(flag)
                    if baseline_flags is None:
                        selected = values[0]
                    else:
                        changed_values = [value for value in set(values) if value != (flag in baseline_flags)]
                        selected = changed_values[0] if changed_values else values[0]
                if selected:
                    selected_flags.add(flag)
            entry.flags = normalize_flags(selected_flags)
            seen = Counter(folder for copy in entry.copies for folder in copy.folders)
            entry.ambiguous = any(count > 1 for count in seen.values())
            entries[key] = entry
        return entries, counts, not index.entries()

    def _reverse_map(self):
        reverse = {}
        folders = self.retry(self.source.folders, self.source) or []
        for folder in folders:
            if self.source.gmail:
                if folder.role == "all":
                    continue
                label = gmail_folder_label(folder)
                if not label:
                    continue
                targets, _ = gmail_real_targets([label], self.options)
                if targets:
                    target = targets[0]
                else:
                    target = {
                        "\\inbox": "@inbox",
                        "\\sent": "@sent",
                        "\\draft": "@drafts",
                        "\\trash": "@trash",
                        "\\spam": "@junk",
                    }.get(label.lower())
            else:
                target = map_source_folder(folder, self.options)
            name = self._resolve(target)
            if name:
                reverse[name] = label if self.source.gmail else folder.name
        for role in ("inbox", "sent", "drafts", "trash", "junk", "archive"):
            if self.source.gmail and role == "archive":
                dest_name = self._resolve("@archive")
                if dest_name:
                    reverse[dest_name] = ""
                continue
            source_folder = next((f for f in folders if f.role == role), None)
            dest_name = self._resolve("@" + role)
            if source_folder and dest_name:
                reverse[dest_name] = (
                    gmail_folder_label(source_folder) or source_folder.name
                    if self.source.gmail
                    else source_folder.name
                )
        return reverse

    def _put_pair(self, key, state, src_folders, src_flags, dst_folders, dst_flags, dst_mixed=()):
        self.store.pairs_put_many(
            self.job_id,
            [
                dict(
                    key=key,
                    state=state,
                    src_folders=src_folders,
                    src_flags=src_flags,
                    dst_folders=dst_folders,
                    dst_flags=dst_flags,
                    dst_mixed=dst_mixed,
                )
            ],
        )

    def _copy_back_target(self, primary, reverse):
        target = reverse.get(primary, primary)
        if self.source.gmail and (
            not target or target.lower() in ("\\inbox", "\\sent", "\\draft", "\\trash", "\\spam")
        ):
            role = {
                "\\inbox": "inbox",
                "\\sent": "sent",
                "\\draft": "drafts",
                "\\trash": "trash",
                "\\spam": "junk",
            }.get(target.lower() if target else "", "all")
            source_folders = self.retry(self.source.folders, self.source) or []
            source_folder = next((folder for folder in source_folders if folder.role == role), None)
            target = source_folder.name if source_folder else "INBOX"
        elif target in ("", None):
            target = "INBOX"
        return target

    def _copy_back_target_is_scanned(self, target):
        if self.source.gmail:
            lowered_target = target.casefold()
            if any(lowered_target == folder.casefold() for folder in self.options.exclude_folders):
                return False
            folders = self.retry(self.source.folders, self.source) or []
            folder = next((item for item in folders if item.name.casefold() == lowered_target), None)
            return not folder or folder.role not in ("junk", "trash")
        scopes = self.retry(self.source.scopes, self.source) or []
        return any(scope.name == target for scope in scopes)

    def _log_copyback_summaries(self):
        if self.unscanned_copybacks:
            self.log(
                f"{self.unscanned_copybacks} destination-only message(s) are in folders that aren't synced back; left alone",
                "warning",
            )
        if self.unsearchable_copybacks:
            self.log(
                f"{self.unsearchable_copybacks} copying-back message(s) without Message-ID were not retried",
                "warning",
            )

    def _copy_back(self, key, entry, reverse, pair=None):
        inbox = self._resolve("@inbox")
        primary = sorted(entry.folders, key=lambda folder: (folder != inbox, folder))[0]
        target = self._copy_back_target(primary, reverse)
        if not self._copy_back_target_is_scanned(target):
            self.unscanned_copybacks += 1
            return
        retrying = bool(pair and pair["state"] == "copying_back")
        if retrying and not key.startswith("mid:"):
            self.unsearchable_copybacks += 1
            return
        if key.startswith("mid:"):
            try:
                if self.source.has_message(target, key):
                    self._put_pair(key, "ignored", [], [], entry.folders, entry.flags, entry.mixed_flags)
                    return
            except Exception as exc:
                self.warning_count += 1
                if self.warning_count <= 20:
                    self.log(
                        f"Could not check whether source already has {key}; will retry: {exc}", "warning"
                    )
                self.failed(str(exc))
                return
        self._put_pair(key, "copying_back", [], [], [primary], entry.flags, entry.mixed_flags)
        copy = next((item for item in entry.copies if primary in item.folders), entry.copies[0])
        try:
            raw = self.retry(lambda: self.dest.fetch_raw(copy.ref), self.dest)
            date = self.retry(lambda: self.dest.fetch_internaldate(copy.ref), self.dest)
            self.source.append(target, raw, entry.flags, date)
        except Exception as exc:
            self.warning_count += 1
            if self.warning_count <= 20:
                self.log(f"Could not copy destination message {key} back; will retry: {exc}", "warning")
            self.failed(str(exc))
            return
        self.progress.increment("__total__", "copied_back")
        self.log(f"Copied destination-only message {key} back to {target}")

    def _apply_destination(self, key, entry, add_flags, remove_flags, add_folders, remove_folders):
        if not (add_flags or remove_flags or add_folders or remove_folders):
            return frozenset()
        if remove_folders and not getattr(self.dest, "can_remove", True):
            raise PermanentError("Destination lacks UIDPLUS; folder removals were skipped")
        for folder in add_folders:
            self.retry(lambda folder=folder: self.dest.resolve(folder, create=True), self.dest)
        flag_ops = []
        for copy in entry.copies:
            add = normalize_flags(add_flags)
            remove = normalize_flags(remove_flags)
            if add or remove:
                flag_ops.append((copy, add, remove))
        change = StateChange(key, entry.copies, flag_ops, list(add_folders), list(remove_folders))
        result = self.retry(
            lambda: self.dest.apply_state([change], retry=lambda operation: self.retry(operation, self.dest)),
            self.dest,
        )
        errors = result if result is not None else {}
        if errors.get(key):
            raise PermanentError(errors[key])
        return normalize_flags(getattr(result, "unsupported_flags", {}).get(key, ()))

    def _merge(self, key, source, destination, pair, reverse):
        old_src_folders = set(pair.get("src_folders") or ())
        old_dst_folders = set(pair.get("dst_folders") or ())
        old_src_flags = normalize_flags(pair.get("src_flags") or ())
        old_dst_flags = normalize_flags(pair.get("dst_flags") or ())
        src_folders, dst_folders = set(source.folders), set(destination.folders)
        src_flags, dst_flags = normalize_flags(source.flags), normalize_flags(destination.flags)
        src_changed, dst_changed = src_folders != old_src_folders, dst_folders != old_dst_folders
        dest_add, dest_remove, source_add, source_remove = set(), set(), set(), set()
        if src_changed and (not dst_changed or src_folders != dst_folders):
            dest_add, dest_remove = src_folders - dst_folders, dst_folders - src_folders
            if not (dst_folders | dest_add) - dest_remove:
                dest_remove.clear()
        elif dst_changed and not src_changed:
            source_add, source_remove = dst_folders - src_folders, src_folders - dst_folders
            if not (src_folders | source_add) - source_remove:
                source_remove.clear()

        dest_add_flags, dest_remove_flags = set(), set()
        source_add_flags, source_remove_flags = set(), set()
        for flag in set(src_flags | dst_flags | old_src_flags | old_dst_flags):
            src_now, dst_now = flag in src_flags, flag in dst_flags
            src_changed_flag = src_now != (flag in old_src_flags)
            dst_changed_flag = dst_now != (flag in old_dst_flags) or flag in destination.changed_copy_flags
            if src_changed_flag and dst_changed_flag:
                desired = src_now or dst_now
                dest_change = source_change = True
            elif src_changed_flag:
                desired = src_now
                dest_change, source_change = True, False
            elif dst_changed_flag:
                desired = dst_now
                dest_change, source_change = False, True
            else:
                continue
            if dest_change:
                (dest_add_flags if desired else dest_remove_flags).add(flag)
            if source_change:
                (source_add_flags if desired else source_remove_flags).add(flag)
        desired_dst_flags = (dst_flags | dest_add_flags) - dest_remove_flags
        for flag in destination.normalize_copy_flags:
            if flag in desired_dst_flags:
                dest_add_flags.add(flag)
                dest_remove_flags.discard(flag)
            else:
                dest_remove_flags.add(flag)
                dest_add_flags.discard(flag)
        try:
            unsupported_dst_flags = self._apply_destination(
                key,
                destination,
                dest_add_flags,
                dest_remove_flags,
                dest_add,
                dest_remove,
            )
            for flag in source_add_flags:
                self.retry(lambda flag=flag: self.source.set_flags(source.locations, [flag], []), self.source)
            for flag in source_remove_flags:
                self.retry(lambda flag=flag: self.source.set_flags(source.locations, [], [flag]), self.source)
            moved_source = False
            if not self.source.gmail and len(source_add) == len(source_remove) == 1:
                removed_folder = next(iter(source_remove))
                added_folder = next(iter(source_add))
                locations = source.folder_locations.get(removed_folder, [])
                source_target = reverse.get(added_folder, added_folder)
                if not locations:
                    raise PermanentError(f"Source message is missing from {removed_folder}")
                move_results = [self.source.move_folder(location, source_target) for location in locations]
                if any(move_results) and not all(move_results):
                    raise PermanentError("Source MOVE only completed for part of a folder move")
                moved_source = bool(move_results) and all(move_results)
            if not moved_source:
                for folder in sorted(source_add):
                    source_folder = reverse.get(folder, folder)
                    self.retry(
                        lambda source_folder=source_folder: self.source.add_folder(
                            source.locations[0], source_folder
                        ),
                        self.source,
                    )
                for folder in sorted(source_remove):
                    mapped = reverse.get(folder, folder)
                    locations = source.folder_locations.get(folder, [])
                    if not locations:
                        raise PermanentError(f"Source message is missing from {folder}")
                    for location in locations:
                        removed = self.retry(
                            lambda location=location, mapped=mapped: self.source.remove_folder(
                                location, mapped
                            ),
                            self.source,
                        )
                        if not removed:
                            raise PermanentError("Source lacks UIDPLUS; folder removal was skipped")
        except Exception as exc:
            self.warning_count += 1
            if self.warning_count <= 20:
                self.log(f"Two-way update for {key} failed and will be retried: {exc}", "warning")
            self.failed(str(exc))
            return
        desired_src_folders = (src_folders | source_add) - source_remove
        desired_dst_folders = (dst_folders | dest_add) - dest_remove
        desired_src_flags = (src_flags | source_add_flags) - source_remove_flags
        desired_dst_flags = (dst_flags | dest_add_flags) - dest_remove_flags
        desired_dst_flags -= unsupported_dst_flags
        remaining_dst_mixed = destination.mixed_flags - (
            destination.normalize_copy_flags - set(unsupported_dst_flags)
        )
        self._put_pair(
            key,
            "synced",
            desired_src_folders,
            desired_src_flags,
            desired_dst_folders,
            desired_dst_flags,
            remaining_dst_mixed,
        )
        if (
            dest_add
            or dest_remove
            or source_add
            or source_remove
            or dest_add_flags
            or dest_remove_flags
            or source_add_flags
            or source_remove_flags
        ):
            self.progress.increment("two-way", "updated")

    def reconcile(self, baseline=False, plan_only=False):
        pairs = self.store.pairs_get_all(self.job_id)
        source, source_counts, source_empty, _, scope_targets = self._source_snapshot(pairs)
        destination, dest_counts, dest_empty = self._destination_snapshot(pairs)
        reverse = self._reverse_map()
        scanned_source_targets = {target for targets in scope_targets.values() for target in targets}
        excluded_source_targets = (
            {
                self._resolve(target)
                for label in self.options.exclude_folders
                for target in gmail_real_targets([label], self.options, ignore_exclusions=True)[0]
            }
            - {None}
            if self.source.gmail
            else set()
        )
        if self.source.gmail:
            if not self.options.include_spam:
                junk_target = self._resolve("@junk")
                if junk_target:
                    excluded_source_targets.add(junk_target)
            if not self.options.include_trash:
                trash_target = self._resolve("@trash")
                if trash_target:
                    excluded_source_targets.add(trash_target)
        ambiguous_keys = set()
        all_keys = set(source) | set(destination) | set(pairs)
        for key in all_keys:
            if (
                source.get(key)
                and source[key].ambiguous
                or destination.get(key)
                and destination[key].ambiguous
            ):
                ambiguous_keys.add(key)
        if ambiguous_keys:
            self.log(f"Skipped {len(ambiguous_keys)} ambiguous two-way message key(s)", "warning")
        if baseline:
            rows = []
            for key in set(source) | set(destination):
                if key in ambiguous_keys:
                    continue
                src, dst = source.get(key), destination.get(key)
                if src and dst and dst.folders:
                    rows.append(
                        dict(
                            key=key,
                            state="synced",
                            src_folders=src.folders,
                            src_flags=src.flags,
                            dst_folders=dst.folders,
                            dst_flags=dst.flags,
                            dst_mixed=dst.mixed_flags,
                        )
                    )
                elif dst and dst.folders:
                    rows.append(
                        dict(
                            key=key,
                            state="ignored",
                            src_folders=[],
                            src_flags=[],
                            dst_folders=dst.folders,
                            dst_flags=dst.flags,
                            dst_mixed=dst.mixed_flags,
                        )
                    )
            self.store.pairs_put_many(self.job_id, rows)
            self.log(
                f"Two-way sync baseline recorded: {len(rows)} messages tracked; changes from now on sync both ways"
            )
            return False
        if plan_only:
            for key in all_keys - ambiguous_keys:
                pair, src, dst = pairs.get(key), source.get(key), destination.get(key)
                if pair is None and dst and dst.folders and not (src and src.folders):
                    self.progress.increment("__total__", "copied_back")
                elif pair and src and src.folders and dst and dst.folders:
                    changed = (
                        set(src.folders) != set(pair.get("src_folders") or ())
                        or set(dst.folders) != set(pair.get("dst_folders") or ())
                        or normalize_flags(src.flags) != normalize_flags(pair.get("src_flags") or ())
                        or normalize_flags(dst.flags) != normalize_flags(pair.get("dst_flags") or ())
                    )
                    if changed:
                        self.progress.increment("__total__", "updated")
                elif pair and ((src and src.folders) != bool(dst and dst.folders)):
                    self.progress.increment("__total__", "trashed")
            return False

        deletions = []
        out_of_scope_pairs = 0
        for key in sorted(all_keys):
            if key in ambiguous_keys:
                continue
            pair, src, dst = pairs.get(key), source.get(key), destination.get(key)
            if pair is None:
                if src and src.folders and dst and dst.folders:
                    self._put_pair(
                        key, "synced", src.folders, src.flags, dst.folders, dst.flags, dst.mixed_flags
                    )
                elif src and src.trash and dst and dst.folders:
                    self._put_pair(key, "ignored", [], [], dst.folders, dst.flags, dst.mixed_flags)
                elif dst and dst.folders:
                    self._copy_back(key, dst, reverse, pair)
                continue
            if pair["state"] == "ignored":
                if src and src.folders and dst and dst.folders:
                    self._put_pair(
                        key, "synced", src.folders, src.flags, dst.folders, dst.flags, dst.mixed_flags
                    )
                continue
            if pair["state"] == "copying_back":
                if src and src.folders:
                    primary = next(iter(pair.get("dst_folders") or ["INBOX"]))
                    self._put_pair(
                        key,
                        "synced",
                        src.folders,
                        src.flags,
                        [primary],
                        dst.flags if dst else pair.get("dst_flags") or [],
                        dst.mixed_flags if dst else pair.get("dst_mixed") or [],
                    )
                elif dst and dst.folders:
                    self._copy_back(key, dst, reverse, pair)
                elif not (src and src.folders):
                    self.store.pairs_delete(self.job_id, [key])
                continue
            if src and src.folders and dst and dst.folders:
                self._merge(key, src, dst, pair, reverse)
            elif src and src.folders and not (dst and dst.folders) and pair.get("dst_folders"):
                folder = sorted(src.folders)[0] if src.folders else "unknown"
                deletions.append(
                    dict(
                        key=key,
                        side="source",
                        folder=folder,
                        folders=set(src.folders),
                        locations=src.locations,
                        copies=[],
                    )
                )
            elif dst and dst.folders and not (src and src.folders) and pair.get("src_folders"):
                pair_source_folders = set(pair.get("src_folders") or ())
                if self.source.gmail:
                    out_of_scope = pair_source_folders <= excluded_source_targets
                else:
                    out_of_scope = not (pair_source_folders & scanned_source_targets)
                if out_of_scope:
                    self.store.pairs_delete(self.job_id, [key])
                    out_of_scope_pairs += 1
                else:
                    folder = sorted(dst.folders)[0]
                    deletions.append(
                        dict(
                            key=key,
                            side="destination",
                            folder=folder,
                            folders=set(dst.folders),
                            locations=[],
                            copies=dst.copies,
                        )
                    )
            elif not (src and src.folders) and not (dst and dst.folders):
                self.store.pairs_delete(self.job_id, [key])

        if out_of_scope_pairs:
            self.log(
                f"{out_of_scope_pairs} message(s) left out of the current source scope; tracking dropped without trashing",
                "info",
            )
        if (source_empty or dest_empty) and pairs:
            self._log_copyback_summaries()
            self.log(
                "A source folder or the destination index looks empty; skipped deletions this run", "warning"
            )
            return False
        if not deletions:
            self.store.update_job(self.job_id, pending_deletions=None)
            self._log_copyback_summaries()
            return False
        total_by_folder = defaultdict(int)
        for item in deletions:
            for folder in item["folders"]:
                total_by_folder[(item["side"], folder)] += 1
        over_percent = any(
            count >= 20 and total_by_folder[(side, folder)] * 100 > count * self.options.max_deletion_percent
            for side, counts in (("source", source_counts), ("destination", dest_counts))
            for folder, count in counts.items()
        )
        exceeds = len(deletions) > self.options.max_deletions or over_percent
        job = self.store.get_job(self.job_id)
        approved = set(job.get("approved_deletions") or ())
        keys = {item["key"] for item in deletions}
        if exceeds and not keys <= approved:
            pending = [{k: item[k] for k in ("key", "side", "folder")} for item in deletions]
            self.store.update_job(self.job_id, pending_deletions=json.dumps(pending))
            self._log_copyback_summaries()
            self.log(
                f"{len(deletions)} messages would be moved to Trash; review and apply on the job page",
                "warning",
            )
            return True
        for item in deletions:
            try:
                if item["side"] == "source":
                    self.retry(lambda item=item: self.source.to_trash(item["locations"]), self.source)
                else:
                    self.retry(lambda item=item: self.dest.to_trash(item["copies"]), self.dest)
                self.store.pairs_delete(self.job_id, [item["key"]])
                self.progress.increment("__total__", "trashed")
            except Exception as exc:
                self.failed(str(exc))
                self.log(f"Could not move {item['key']} to Trash; will retry: {exc}", "warning")
        self.store.update_job(
            self.job_id,
            pending_deletions=None,
            approved_deletions=None,
        )
        self._log_copyback_summaries()
        return False
