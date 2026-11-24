"""Bidirectional reconciliation for CardDAV contacts and CalDAV calendars."""

import json
import re
import time
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from datetime import time as datetime_time

from icalendar import Calendar

from emailsync import db
from emailsync.dav.source import hash_for_uid
from emailsync.dav.vobjects import content_hash
from emailsync.errors import PermanentError


def _safe_filename(uid: str, kind: str) -> str:
    extension = ".vcf" if kind == "contacts" else ".ics"
    safe_uid = re.sub(r"[^A-Za-z0-9._@+-]", "_", uid)[:180] or "item"
    return safe_uid + extension


def _as_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime_time.min, tzinfo=UTC)
    elif isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        parsed = None
        for candidate in (value, value.replace("Z", "+00:00")):
            try:
                parsed = datetime.fromisoformat(candidate)
                break
            except ValueError:
                continue
        if parsed is None:
            for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%d"):
                try:
                    parsed = datetime.strptime(value, fmt)
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp(data: bytes | str, kind: str) -> datetime | None:
    text = data.decode("utf-8", "replace") if isinstance(data, bytes) else data
    if kind == "contacts":
        unfolded = re.sub(r"\r?\n[ \t]", "", text)
        for line in unfolded.splitlines():
            if line.split(":", 1)[0].split(";", 1)[0].strip().upper() == "REV" and ":" in line:
                return _as_datetime(line.split(":", 1)[1].strip())
        return None
    try:
        calendar = Calendar.from_ical(text)
    except Exception:
        return None
    values = []
    for component in calendar.walk():
        if component.name not in ("VEVENT", "VTODO", "VJOURNAL"):
            continue
        value = component.get("last-modified")
        if value is None:
            continue
        try:
            value = value.dt if hasattr(value, "dt") else value
            parsed = _as_datetime(value)
        except (TypeError, ValueError, AttributeError):
            parsed = None
        if parsed is not None:
            values.append(parsed)
    return max(values) if values else None


def _subject(data: bytes | str, kind: str) -> str:
    text = data.decode("utf-8", "replace") if isinstance(data, bytes) else data
    unfolded = re.sub(r"\r?\n[ \t]", "", text)
    wanted = "FN" if kind == "contacts" else "SUMMARY"
    for line in unfolded.splitlines():
        if line.split(":", 1)[0].split(";", 1)[0].strip().upper() == wanted and ":" in line:
            return line.split(":", 1)[1].strip()
    return wanted


class DavTwoWayReconciler:
    def __init__(
        self, store, job_id, kind, source, dest, options, retry, progress, log, failed, control=None
    ):
        if kind not in ("contacts", "calendars"):
            raise ValueError("kind must be contacts or calendars")
        self.store = store
        self.job_id = job_id
        self.kind = kind
        self.source = source
        self.dest = dest
        self.options = options
        self.retry = retry
        self.progress = progress
        self.log = log or (lambda _level, _message: None)
        self.failed = failed or (lambda _error: None)
        self.control = control
        self._blocked_copybacks = 0

    def _stopped(self):
        return self.control is not None and (self.control.stopped or self.control.aborted)

    def _call(self, operation, connection):
        if self.retry:
            return self.retry(operation, connection)
        return operation()

    def _target_for(self, scope):
        collections = self.dest.collections
        if self.kind == "contacts":
            return next(
                (item for item in collections if item["displayname"].casefold() == "default"),
                collections[0] if collections else None,
            )
        name = scope.name.strip().casefold()
        return next(
            (item for item in collections if item["displayname"].strip().casefold() == name),
            None,
        )

    @staticmethod
    def _scope_name_for_key(key, names):
        matches = [name for name in names if key.startswith(name + "|")]
        return max(matches, key=len) if matches else None

    def _source_snapshot(self):
        scopes = self._call(self.source.scopes, self.source) or []
        source_by_key = {}
        source_by_target_uid = defaultdict(list)
        targets = {}
        scope_items = {}
        ambiguous = set()
        failed_scopes = set()

        for scope in scopes:
            target = self._target_for(scope)
            targets[scope.name] = target
            try:
                items = (
                    self._call(lambda scope=scope: self.source.items(scope, strict=True), self.source) or []
                )
            except Exception as exc:
                self._failed(exc)
                self.log("warning", f"Could not snapshot DAV source collection {scope.name}: {exc}")
                items = []
                failed_scopes.add(scope.name)
            scope_items[scope.name] = items
            counts = Counter(item.uid for item in items)
            for item in items:
                key = f"{scope.name}|{item.uid}"
                record = {
                    "key": key,
                    "scope": scope,
                    "target": target,
                    "item": item,
                    "hash": content_hash(item.data),
                    "timestamp": _timestamp(item.data, self.kind),
                }
                if item.ambiguous or counts[item.uid] > 1:
                    ambiguous.add(key)
                if key in source_by_key:
                    ambiguous.add(key)
                else:
                    source_by_key[key] = record
                if target is not None:
                    source_by_target_uid[(target["href"], item.uid)].append(record)

        for records in source_by_target_uid.values():
            if len({record["scope"].name for record in records}) > 1:
                for record in records:
                    ambiguous.add(record["key"])
        return scopes, source_by_key, source_by_target_uid, targets, scope_items, ambiguous, failed_scopes

    def _snapshot(self, pairs):
        (
            scopes,
            source_by_key,
            source_by_target_uid,
            targets,
            scope_items,
            ambiguous,
            failed_scopes,
        ) = self._source_snapshot()
        dest_by_collection_uid = {}
        dest_items_by_collection = {}
        ambiguous_dest = set()
        collection_targets = {}
        failed_collections = set()
        available_collections = {item["href"]: item for item in self.dest.collections}
        for target in targets.values():
            if target is None:
                continue
            href = target["href"]
            collection_targets[href] = target
        mapped_hrefs = set(collection_targets)
        for pair in pairs.values():
            href = pair.get("dst_href")
            if href in available_collections:
                collection_targets[href] = available_collections[href]
        for href, collection in collection_targets.items():
            try:
                items = (
                    self._call(
                        lambda collection=collection: self.dest.items(collection, strict=True),
                        self.dest,
                    )
                    or []
                )
            except Exception as exc:
                self._failed(exc)
                self.log(
                    "warning",
                    f"Could not snapshot DAV destination collection {collection['displayname']}: {exc}",
                )
                items = []
                failed_collections.add(href)
            dest_items_by_collection[href] = items
            counts = Counter(item.uid for item in items)
            for item in items:
                record = {
                    "item": item,
                    "target": collection,
                    "hash": content_hash(item.data),
                    "timestamp": _timestamp(item.data, self.kind),
                }
                key = (href, item.uid)
                if item.ambiguous or counts[item.uid] > 1:
                    ambiguous_dest.add(key)
                    for source_record in source_by_target_uid.get(key, []):
                        ambiguous.add(source_record["key"])
                if key in dest_by_collection_uid:
                    ambiguous_dest.add(key)
                    for source_record in source_by_target_uid.get(key, []):
                        ambiguous.add(source_record["key"])
                else:
                    dest_by_collection_uid[key] = record
        scope_names = [scope.name for scope in scopes]
        guarded = set()
        for key, pair in pairs.items():
            if pair["state"] == "ignored":
                continue
            scope_name = self._scope_name_for_key(key, scope_names)
            if scope_name is None:
                guarded.add("missing source scope")
                continue
            if scope_name in failed_scopes:
                guarded.add("failed source snapshot")
                continue
            target = targets.get(scope_name)
            href = pair.get("dst_href") or (target["href"] if target else None)
            if href is None:
                guarded.add("missing mapped destination collection")
                continue
            if href not in available_collections:
                guarded.add("missing paired destination collection")
                continue
            if href in failed_collections:
                guarded.add("failed destination snapshot")
                continue
            if not dest_items_by_collection.get(href):
                guarded.add("empty destination collection")
            if not scope_items.get(scope_name):
                guarded.add("empty source scope")

        discovered_hrefs = set(available_collections)
        unmapped = discovered_hrefs - mapped_hrefs
        if unmapped:
            self.log(
                "info",
                f"{len(unmapped)} destination collection(s) have no source mapping and were left alone",
            )
        if ambiguous or ambiguous_dest:
            self.log(
                "warning",
                f"{len(ambiguous) + len(ambiguous_dest)} ambiguous DAV item(s) were skipped",
            )
        if guarded:
            self.log(
                "warning", "DAV deletions were suppressed because a paired collection was empty or missing"
            )
        return {
            "scopes": scopes,
            "source": source_by_key,
            "source_by_target_uid": source_by_target_uid,
            "targets": targets,
            "scope_items": scope_items,
            "destination": dest_by_collection_uid,
            "destination_items": dest_items_by_collection,
            "collection_targets": collection_targets,
            "ambiguous": ambiguous,
            "ambiguous_dest": ambiguous_dest,
            "guarded": guarded,
            "failed_scopes": failed_scopes,
            "failed_collections": failed_collections,
        }

    def _pair(self, key, state, src_hash=None, dst_hash=None, dst_href=None):
        if self._stopped():
            return False
        self.store.dav_pairs_put_many(
            self.job_id,
            self.kind,
            [
                {
                    "key": key,
                    "state": state,
                    "src_hash": src_hash,
                    "dst_hash": dst_hash,
                    "dst_href": dst_href,
                }
            ],
        )
        return True

    def _delete_pair(self, key):
        self.store.dav_pairs_delete(self.job_id, self.kind, [key])

    def _update_ledger_baseline(self, key, src_hash, uid):
        if self._stopped():
            return
        rows = self.store.ledger_get_many(self.job_id, self.kind, [key])
        row = rows.get(key)
        if not row:
            return
        info = row.get("info")
        try:
            parsed = json.loads(info) if info else None
        except (TypeError, json.JSONDecodeError):
            parsed = None
        fingerprint = parsed.get("fp") if isinstance(parsed, dict) else info
        info = json.dumps({"fp": fingerprint, "hash": src_hash}, sort_keys=True)
        self.store.ledger_mark(
            self.job_id,
            self.kind,
            key,
            row["status"],
            match_key=uid,
            targets=row.get("targets"),
            info=info,
        )

    def _failed(self, exc):
        try:
            self.failed(exc)
        except Exception as callback_error:
            self.log("warning", f"Could not record DAV reconciliation failure: {callback_error}")
        self.log("warning", f"DAV two-way operation failed: {exc}")

    def _progress(self, scope, field):
        if not self.progress:
            return
        try:
            self.progress.update(scope, field)
        except (KeyError, AttributeError):
            pass

    def _task_progress(self, field):
        if not self.progress:
            return
        method = getattr(self.progress, "increment_task", None)
        if method:
            method(field)

    def _read_after_write(self, endpoint, ref, uid):
        try:
            result = self._call(lambda: endpoint.get(ref), endpoint)
            if self._stopped():
                return None
            return hash_for_uid(result[1], self.kind, uid) if result else None
        except Exception as exc:
            self.log("warning", f"Could not read back DAV object {ref}: {exc}")
            return None

    def _push_to_destination(self, key, source_record, dest_record):
        if self._stopped():
            return False
        source_item = source_record["item"]
        dest_item = dest_record["item"]
        try:
            updated = self._call(
                lambda: self.dest.update(
                    dest_record["target"],
                    dest_item.ref,
                    dest_item.etag,
                    source_item.data,
                ),
                self.dest,
            )
        except Exception as exc:
            self._failed(exc)
            return False
        if updated is not True:
            if updated is False:
                self.log("warning", f"DAV item {key} changed during the run; will retry")
            return False
        dst_hash = self._read_after_write(self.dest, dest_item.ref, source_item.uid)
        if self._stopped() or not self._pair(
            key,
            "synced",
            source_record["hash"],
            dst_hash,
            dest_record["target"]["href"],
        ):
            return False
        self._update_ledger_baseline(key, source_record["hash"], source_item.uid)
        self._progress(source_record["scope"].name, "updated")
        return True

    def _push_to_source(self, key, source_record, dest_record):
        if self._stopped():
            return False
        source_item = source_record["item"]
        dest_item = dest_record["item"]
        try:
            updated = self._call(
                lambda: self.source.update(
                    source_record["scope"].collection,
                    source_item.ref,
                    source_item.etag,
                    dest_item.data,
                ),
                self.source,
            )
        except Exception as exc:
            self._failed(exc)
            return False
        if updated is not True:
            if updated is False:
                self.log("warning", f"DAV item {key} changed during the run; will retry")
            return False
        src_hash = self._read_after_write(self.source, source_item.ref, source_item.uid)
        if self._stopped() or not self._pair(
            key,
            "synced",
            src_hash,
            dest_record["hash"],
            dest_record["target"]["href"],
        ):
            return False
        self._update_ledger_baseline(key, src_hash, source_item.uid)
        self._progress(source_record["scope"].name, "updated")
        return True

    def _copy_back(self, key, scope, target, dest_record, plan_only=False):
        item = dest_record["item"]
        if plan_only:
            self._task_progress("copied_back")
            return
        if self._stopped() or not self._pair(
            key,
            "copying_back",
            None,
            dest_record["hash"],
            dest_record["target"]["href"],
        ):
            return
        try:
            created = self._call(
                lambda: self.source.put(
                    scope.collection,
                    _safe_filename(item.uid, self.kind),
                    item.data,
                ),
                self.source,
            )
        except PermanentError as exc:
            if "403" in str(exc) or "409" in str(exc):
                self._blocked_copybacks += 1
                return
            self._failed(exc)
            return
        except Exception as exc:
            self._failed(exc)
            return
        if not created:
            return
        src_hash = self._read_after_write(self.source, created, item.uid)
        if self._stopped() or not self._pair(
            key,
            "synced",
            src_hash,
            dest_record["hash"],
            dest_record["target"]["href"],
        ):
            return
        source_collection = scope.collection.get("displayname", scope.name)
        info = json.dumps({"fp": None, "hash": src_hash}, sort_keys=True)
        self.store.ledger_mark(
            self.job_id,
            self.kind,
            key,
            db.SKIPPED,
            match_key=item.uid,
            targets=[source_collection],
            info=info,
        )
        self._task_progress("copied_back")
        self.log("info", f"Copied back DAV item {key}")

    def _deletion_entry(self, key, side, scope, target, record):
        collection = scope.collection if side == "source" else target
        item = record["item"]
        return {
            "key": key,
            "side": side,
            "scope": scope,
            "target": target,
            "record": record,
            "collection": collection,
            "collection_name": collection.get("displayname", scope.name),
            "collection_key": collection.get("href", collection.get("displayname", scope.name)),
            "subject": _subject(item.data, self.kind),
        }

    def _deletions_exceed_limit(self, deletions, snapshot):
        if len(deletions) > self.options.max_deletions:
            return True
        planned = Counter()
        totals = {}
        for deletion in deletions:
            group = (deletion["side"], deletion["collection_key"])
            planned[group] += 1
            if deletion["side"] == "source":
                totals[group] = len(snapshot["scope_items"].get(deletion["scope"].name, []))
            else:
                href = deletion["target"]["href"]
                totals[group] = len(snapshot["destination_items"].get(href, []))
        for group, count in planned.items():
            total = totals[group]
            if total >= 20 and count * 100 > self.options.max_deletion_percent * total:
                return True
        return False

    def _apply_deletions(self, deletions, snapshot, plan_only):
        if self._stopped():
            return False
        if not deletions:
            if not plan_only:
                self.store.set_pending_deletions(self.job_id, self.kind, [])
            return False
        if plan_only:
            for _deletion in deletions:
                self._task_progress("trashed")
            return False
        exceeded = self._deletions_exceed_limit(deletions, snapshot)
        planned = {(item["key"], item["side"]) for item in deletions}
        approved = self.store.get_approved(self.job_id, self.kind)
        if exceeded and not planned <= approved:
            pending = [
                {
                    "kind": self.kind,
                    "key": item["key"],
                    "side": item["side"],
                    "folder": item["collection_name"],
                    "subject": item["subject"],
                }
                for item in deletions
            ]
            self.store.set_pending_deletions(self.job_id, self.kind, pending)
            self.log(
                "warning",
                f"{len(deletions)} DAV item(s) would be deleted; review and apply them on the job page",
            )
            return True

        for deletion in deletions:
            if self._stopped():
                break
            key = deletion["key"]
            item = deletion["record"]["item"]
            try:
                archived_id = self.store.dav_deleted_add(
                    self.job_id,
                    self.kind,
                    key,
                    deletion["side"],
                    deletion["collection_name"],
                    item.data,
                )
                if self._stopped():
                    self.store.dav_deleted_remove(archived_id)
                    break
                if deletion["side"] == "source":
                    deleted = self._call(
                        lambda ref=item.ref, etag=item.etag: self.source.delete(ref, etag), self.source
                    )
                else:
                    deleted = self._call(
                        lambda ref=item.ref, etag=item.etag: self.dest.delete(ref, etag), self.dest
                    )
            except Exception as exc:
                self._failed(exc)
                continue
            if deleted is not True:
                if deleted is None:
                    self.store.dav_deleted_remove(archived_id)
                elif deleted is False:
                    self.log("warning", f"DAV item {key} changed during the run; will retry")
                continue
            self._delete_pair(key)
            self.store.ledger_delete(self.job_id, self.kind, [key])
            self._task_progress("trashed")
        if not self._stopped():
            self.store.clear_deletions(self.job_id, self.kind)
        return False

    def reconcile(self, baseline=False, plan_only=False):
        if self._stopped():
            return False
        pairs = self.store.dav_pairs_get_all(self.job_id, self.kind)
        snapshot = self._snapshot(pairs)
        source = snapshot["source"]
        destination = snapshot["destination"]
        ambiguous = snapshot["ambiguous"]

        if baseline:
            if snapshot["failed_scopes"] or snapshot["failed_collections"]:
                self.log(
                    "warning",
                    f"The initial two-way {self.kind} baseline was not recorded because a DAV snapshot was incomplete; it will be retried next run",
                )
                return False
            if not plan_only:
                if self._stopped():
                    return False
                writes = []
                for key, source_record in source.items():
                    if self._stopped():
                        return False
                    if key in ambiguous:
                        continue
                    target = source_record["target"]
                    if target is None:
                        continue
                    dest_record = destination.get((target["href"], source_record["item"].uid))
                    if dest_record:
                        writes.append(
                            {
                                "key": key,
                                "state": "synced",
                                "src_hash": source_record["hash"],
                                "dst_hash": dest_record["hash"],
                                "dst_href": dest_record["target"]["href"],
                            }
                        )
                for (href, uid), dest_record in destination.items():
                    if self._stopped():
                        return False
                    if snapshot["source_by_target_uid"].get((href, uid)):
                        continue
                    candidates = [
                        scope
                        for scope in snapshot["scopes"]
                        if snapshot["targets"].get(scope.name) is not None
                        and snapshot["targets"][scope.name]["href"] == href
                    ]
                    if not candidates:
                        continue
                    scope = next(
                        (item for item in candidates if item.name.casefold() == "default"), candidates[0]
                    )
                    key = f"{scope.name}|{uid}"
                    if key not in ambiguous and (href, uid) not in snapshot["ambiguous_dest"]:
                        writes.append(
                            {
                                "key": key,
                                "state": "ignored",
                                "src_hash": None,
                                "dst_hash": dest_record["hash"],
                                "dst_href": href,
                            }
                        )
                if self._stopped():
                    return False
                self.store.dav_pairs_put_many(self.job_id, self.kind, writes)
                self.store.set_task_baseline(self.job_id, self.kind, time.time())
                self.log("info", f"Recorded the initial two-way {self.kind} baseline")
            return False

        deletions = []
        handled_keys = set()
        for key, pair in pairs.items():
            if self._stopped():
                break
            if key in ambiguous or pair["state"] == "ignored":
                continue
            scope_name = self._scope_name_for_key(key, [scope.name for scope in snapshot["scopes"]])
            scope = next((item for item in snapshot["scopes"] if item.name == scope_name), None)
            if scope is None:
                continue
            target = snapshot["targets"].get(scope.name)
            dst_href = pair.get("dst_href") or (target["href"] if target else None)
            pair_target = snapshot["collection_targets"].get(dst_href) if dst_href else None
            if pair_target is None:
                continue
            src_record = source.get(key)
            dst_record = destination.get((dst_href, key[len(scope.name) + 1 :]))
            uid = key[len(scope.name) + 1 :]
            if (dst_href, uid) in snapshot["ambiguous_dest"]:
                continue
            handled_keys.add(key)

            if pair["state"] == "copying_back":
                if src_record and dst_record:
                    if not plan_only:
                        self._pair(key, "synced", src_record["hash"], dst_record["hash"], dst_href)
                        self._update_ledger_baseline(key, src_record["hash"], uid)
                    continue
                if not src_record and dst_record:
                    self._copy_back(key, scope, pair_target, dst_record, plan_only=plan_only)
                    continue
                if src_record and not dst_record:
                    if not plan_only:
                        self._pair(key, "ignored", src_record["hash"], None, dst_href)
                    continue
                if not snapshot["guarded"] and not plan_only:
                    self._delete_pair(key)
                continue

            if src_record and dst_record:
                if pair["state"] != "synced":
                    if not plan_only:
                        self._pair(key, "synced", src_record["hash"], dst_record["hash"], dst_href)
                    continue
                src_changed = pair["src_hash"] is not None and src_record["hash"] != pair["src_hash"]
                dst_changed = pair["dst_hash"] is not None and dst_record["hash"] != pair["dst_hash"]
                if not src_changed and not dst_changed:
                    if not plan_only:
                        self._pair(key, "synced", src_record["hash"], dst_record["hash"], dst_href)
                        self._update_ledger_baseline(key, src_record["hash"], uid)
                    continue
                if src_changed and dst_changed:
                    if src_record["hash"] == dst_record["hash"]:
                        if not plan_only:
                            self._pair(key, "synced", src_record["hash"], dst_record["hash"], dst_href)
                            self._update_ledger_baseline(key, src_record["hash"], uid)
                        continue
                    src_timestamp, dst_timestamp = source[key]["timestamp"], dst_record["timestamp"]
                    if src_timestamp is None or dst_timestamp is None or src_timestamp >= dst_timestamp:
                        if plan_only:
                            self._progress(scope.name, "updated")
                        else:
                            self._push_to_destination(key, src_record, dst_record)
                    else:
                        if plan_only:
                            self._progress(scope.name, "updated")
                        else:
                            self._push_to_source(key, src_record, dst_record)
                elif src_changed:
                    if plan_only:
                        self._progress(scope.name, "updated")
                    else:
                        self._push_to_destination(key, src_record, dst_record)
                else:
                    if plan_only:
                        self._progress(scope.name, "updated")
                    else:
                        self._push_to_source(key, src_record, dst_record)
                continue

            if src_record and not dst_record and pair["state"] == "synced":
                if not snapshot["guarded"]:
                    deletions.append(self._deletion_entry(key, "source", scope, pair_target, src_record))
                continue
            if dst_record and not src_record and pair["state"] == "synced":
                if any(
                    record["key"] != key
                    for record in snapshot["source_by_target_uid"].get((dst_href, uid), [])
                ):
                    continue
                if not snapshot["guarded"]:
                    deletions.append(self._deletion_entry(key, "destination", scope, pair_target, dst_record))
                continue
            if (
                not src_record
                and not dst_record
                and not snapshot["guarded"]
                and not plan_only
                and not self._stopped()
            ):
                self._delete_pair(key)

        for key, source_record in source.items():
            if self._stopped():
                break
            if key in pairs or key in ambiguous or source_record["target"] is None:
                continue
            target = source_record["target"]
            uid = source_record["item"].uid
            dest_record = destination.get((target["href"], uid))
            if dest_record and (target["href"], uid) not in snapshot["ambiguous_dest"] and not plan_only:
                self._pair(
                    key,
                    "synced",
                    source_record["hash"],
                    dest_record["hash"],
                    dest_record["target"]["href"],
                )
                self._update_ledger_baseline(key, source_record["hash"], uid)

        for (href, uid), dest_record in destination.items():
            if self._stopped():
                break
            source_records = snapshot["source_by_target_uid"].get((href, uid), [])
            if source_records:
                continue
            scopes = [
                scope
                for scope in snapshot["scopes"]
                if snapshot["targets"].get(scope.name) is not None
                and snapshot["targets"][scope.name]["href"] == href
            ]
            if not scopes:
                continue
            scope = next((item for item in scopes if item.name.casefold() == "default"), scopes[0])
            key = f"{scope.name}|{uid}"
            if key in ambiguous or key in handled_keys:
                continue
            if (href, uid) in snapshot["ambiguous_dest"]:
                continue
            matching_pair = pairs.get(key)
            if matching_pair:
                continue
            self._copy_back(key, scope, snapshot["targets"][scope.name], dest_record, plan_only=plan_only)

        if self._blocked_copybacks:
            self.log(
                "warning",
                f"{self._blocked_copybacks} destination-only DAV item(s) could not be copied back; "
                "they remain tracked for retry",
            )
        return self._apply_deletions(deletions, snapshot, plan_only)
