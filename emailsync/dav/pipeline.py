"""Contacts and calendar migration over CardDAV, CalDAV, and local Takeout files."""

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from emailsync import db
from emailsync.connectors.dav import DavClient
from emailsync.dav.source import DavSource
from emailsync.errors import AuthError, FatalError, wrap
from emailsync.jobs.control import TaskResult
from emailsync.jobs.retry import retry_call
from emailsync.options import JobConfig


def _make_source(endpoint, kind):
    return DavSource(endpoint, kind)


def _make_dest(endpoint, kind):
    return DavDestination(endpoint, kind)


class DavDestination:
    def __init__(self, endpoint, kind):
        self.endpoint = endpoint
        self.kind = kind
        self.client = None
        self.home = ""
        self.collections = []

    def connect(self):
        self.client = DavClient(self.endpoint, "carddav" if self.kind == "contacts" else "caldav")
        self.home, self.collections = self.client.discover()
        return self

    def reconnect(self):
        self.close()
        return self.connect()

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

    def create_collection(self, name):
        collection = self.client.create_collection(name)
        self.collections.append(collection)
        return collection

    def items(self, collection):
        from emailsync.dav.source import _decode_objects

        out = []
        for _href, _etag, data in self.client.list_objects(collection):
            out.extend(_decode_objects(data, self.kind))
        return out

    def put(self, collection, filename, data):
        return self.client.put(collection, filename, data)


class _DavProgress:
    def __init__(self, store, job_id, kind):
        self.store = store
        self.job_id = job_id
        self.kind = kind
        self.lock = threading.Lock()
        self.values = dict(total=0, already=0, copied=0, skipped=0, failed=0)
        self.scopes = {}
        self.message = "Connecting"
        self.last_flush = 0

    def set_message(self, message):
        with self.lock:
            self.message = message
        self.flush()

    def set_scopes(self, names):
        with self.lock:
            self.scopes = {
                name: dict(total=0, already=0, copied=0, skipped=0, failed=0, status="pending")
                for name in names
            }
        self.flush(force=True)

    def total(self, name, amount):
        with self.lock:
            row = self.scopes[name]
            self.values["total"] += amount - row["total"]
            row["total"] = amount
        self.flush()

    def update(self, name, field, amount=1, status=None):
        with self.lock:
            self.values[field] += amount
            self.scopes[name][field] += amount
            if status:
                self.scopes[name]["status"] = status
        self.flush()

    def status(self, name, value):
        with self.lock:
            self.scopes[name]["status"] = value
        self.flush()

    def flush(self, force=False):
        with self.lock:
            now = time.monotonic()
            if not force and now - self.last_flush < 2:
                return
            self.last_flush = now
            values = dict(self.values)
            scopes = {key: dict(value) for key, value in self.scopes.items()}
            message = self.message
        self.store.update_task(self.job_id, self.kind, **values, message=message)
        self.store.save_scopes(self.job_id, self.kind, scopes)


class DavMigration:
    def __init__(
        self,
        store,
        job_id: int,
        kind: str,
        config: JobConfig,
        control,
        log=None,
        source_factory=None,
        dest_factory=None,
    ):
        if kind not in ("contacts", "calendars"):
            raise ValueError("kind must be contacts or calendars")
        self.store = store
        self.job_id = job_id
        self.kind = kind
        self.config = config
        self.control = control
        self.log = log or (lambda level, message: store.add_event(job_id, level, message))
        self.source_factory = source_factory or _make_source
        self.dest_factory = dest_factory or _make_dest
        self.options = config.options
        self.progress = _DavProgress(store, job_id, kind)
        self.failed = 0
        self._failed_lock = threading.Lock()
        self._destination_lock = threading.Lock()

    def _retry(self, operation, connection=None):
        return retry_call(
            operation,
            self.control,
            connection=connection,
            on_wait=self.progress.set_message,
            on_retry=lambda attempt, error: self.log(
                "warning", f"Temporary server error; retry {attempt}: {error}"
            ),
        )

    def _items_fingerprint(self, item):
        return item.fingerprint

    def _get_fingerprint(self, item):
        return self._items_fingerprint(item)

    def _index_collection(self, dest, collection):
        uid_index = set()
        fingerprint_index = set()
        for item in self._retry(lambda: dest.items(collection), dest) or []:
            uid_index.add(item.uid)
            fingerprint_index.add(self._get_fingerprint(item))
        return {"collection": collection, "uids": uid_index, "fingerprints": fingerprint_index}

    def _target_for(self, dest, scope):
        collections = dest.collections
        if self.kind == "contacts":
            target = next(
                (c for c in collections if c["displayname"].casefold() == "default"),
                collections[0] if collections else None,
            )
            if target is None and not self.options.dry_run:
                target = self._retry(lambda: dest.create_collection("Default"), dest)
                collections = dest.collections
            return target or {"href": dest.home, "displayname": "Default"}
        source_name = scope.name.strip()
        target = next((c for c in collections if c["displayname"].casefold() == source_name.casefold()), None)
        if target is None and not self.options.dry_run:
            target = self._retry(lambda: dest.create_collection(source_name or "Calendar"), dest)
        return target or {"href": dest.home, "displayname": source_name or "Calendar"}

    def _record_failure(self, scope, key, uid, target, error):
        self.store.ledger_mark(
            self.job_id,
            self.kind,
            key,
            db.FAILED,
            match_key=uid,
            targets=[target["displayname"]],
            error=str(error)[:1000],
        )
        self.progress.update(scope, "failed")
        with self._failed_lock:
            self.failed += 1

    def _process(self, scope, item, target, indexed, source_key, collection_name):
        if self.control.stopped or self.control.aborted:
            return
        with self._destination_lock:
            if self.control.stopped or self.control.aborted:
                return
            fingerprint = self._get_fingerprint(item)
            exists = item.uid in indexed["uids"] or (
                self.options.dav_match_fingerprint and fingerprint in indexed["fingerprints"]
            )
            if exists:
                self.store.ledger_mark(
                    self.job_id,
                    self.kind,
                    source_key,
                    db.SKIPPED,
                    match_key=item.uid,
                    targets=[collection_name],
                    info=fingerprint,
                )
                indexed["uids"].add(item.uid)
                indexed["fingerprints"].add(fingerprint)
                self.progress.update(scope.name, "skipped")
                return
            extension = ".vcf" if self.kind == "contacts" else ".ics"
            safe_uid = re.sub(r"[^A-Za-z0-9._@+-]", "_", item.uid)[:180] or "item"
            filename = safe_uid + extension
            self.store.ledger_mark(
                self.job_id,
                self.kind,
                source_key,
                db.UPLOADING,
                match_key=item.uid,
                targets=[collection_name],
                info=fingerprint,
                count_attempt=True,
            )
            created = self._retry(lambda: self.dest.put(target, filename, item.data), self.dest)
            if self.control.stopped or self.control.aborted:
                return
            if created:
                status = db.DONE
                self.progress.update(scope.name, "copied")
            else:
                status = db.SKIPPED
                self.progress.update(scope.name, "skipped")
            self.store.ledger_mark(
                self.job_id,
                self.kind,
                source_key,
                status,
                match_key=item.uid,
                targets=[collection_name],
                info=fingerprint,
            )
            indexed["uids"].add(item.uid)
            indexed["fingerprints"].add(fingerprint)

    def run(self) -> TaskResult:
        self.store.reset_task(self.job_id, self.kind, "running")
        self.progress.flush(force=True)
        source = self.source_factory(self.config.endpoints[self.kind]["source"], self.kind)
        self.dest = self.dest_factory(self.config.endpoints[self.kind]["dest"], self.kind)
        executor = None
        try:
            self.progress.set_message("Connecting to source and destination")
            self._retry(source.connect, source)
            self._retry(self.dest.connect, self.dest)
            scopes = self._retry(source.scopes, source) or []
            self.progress.set_scopes([scope.name for scope in scopes])
            indexed_collections = [
                self._index_collection(self.dest, collection) for collection in self.dest.collections
            ]
            work = []
            for scope in scopes:
                if self.control.stopped or self.control.aborted:
                    break
                self.progress.set_message(f"Scanning {scope.name}")
                items = self._retry(lambda scope=scope: source.items(scope), source) or []
                self.progress.total(scope.name, len(items))
                target = self._target_for(self.dest, scope)
                indexed = next(
                    (index for index in indexed_collections if index["collection"]["href"] == target["href"]),
                    None,
                )
                if indexed is None:
                    indexed = {"collection": target, "uids": set(), "fingerprints": set()}
                    indexed_collections.append(indexed)
                self.progress.status(scope.name, "running")
                for item in items:
                    if self.control.stopped or self.control.aborted:
                        break
                    source_key = f"{scope.name}|{item.uid}"
                    row = self.store.ledger_get_many(self.job_id, self.kind, [source_key]).get(source_key)
                    if row and row["status"] in db.SETTLED:
                        self.progress.update(scope.name, "already")
                    else:
                        work.append((scope, item, target, indexed, source_key, target["displayname"]))
                self.progress.status(scope.name, "done")
            if not self.control.stopped and not self.control.aborted:
                self.progress.set_message("Copying")
                executor = ThreadPoolExecutor(
                    max_workers=self.options.workers, thread_name_prefix=f"dav-{self.kind}"
                )
                futures = [
                    executor.submit(self._process, scope, item, target, indexed, source_key, collection_name)
                    for scope, item, target, indexed, source_key, collection_name in work
                ]
                for (scope, item, target, _indexed, source_key, _collection_name), future in zip(
                    work, futures, strict=False
                ):
                    try:
                        future.result()
                    except (AuthError, FatalError) as exc:
                        self.control.abort(exc)
                    except BaseException as exc:
                        self.log("error", str(wrap(exc)))
                        self._record_failure(scope.name, source_key, item.uid, target, exc)
            if self.control.aborted:
                error = str(self.control.error or "Task aborted")
                status = "failed"
            elif self.control.stopped:
                error = None
                status = "paused" if self.control.reason == "pause" else "cancelled"
            else:
                error = None
                status = "completed_with_errors" if self.failed else "completed"
            self.progress.set_message(status.replace("_", " ").capitalize())
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id,
                self.kind,
                status=status,
                message=error or self.progress.message,
                finished_at=time.time(),
            )
            return TaskResult(status, error)
        except (AuthError, FatalError) as exc:
            error = str(exc)
            self.log("error", error)
            self.control.abort(exc)
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id, self.kind, status="failed", message=error, finished_at=time.time()
            )
            return TaskResult("failed", error)
        except BaseException as exc:
            error = str(wrap(exc))
            self.log("error", error)
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id, self.kind, status="failed", message=error, finished_at=time.time()
            )
            return TaskResult("failed", error)
        finally:
            if executor:
                executor.shutdown(wait=True, cancel_futures=True)
            for obj in (source, self.dest):
                try:
                    obj.close()
                except Exception:
                    pass
