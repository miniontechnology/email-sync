"""Crash-safe coordinator and worker pool for mail migrations."""

import queue
import threading
import time
from dataclasses import dataclass

from emailsync import db
from emailsync.errors import AuthError, FatalError, wrap
from emailsync.jobs.control import RunControl, TaskResult
from emailsync.jobs.retry import retry_call
from emailsync.mail.dest import make_dest
from emailsync.mail.index import DestIndex
from emailsync.mail.source import make_source
from emailsync.options import JobConfig

MAX_BATCH_MESSAGES = 25
MAX_BATCH_BYTES = 20 * 1024 * 1024
META_BATCH_SIZE = 500
FLUSH_SECONDS = 2.0


@dataclass
class _WorkItem:
    scope: object
    uid: int
    source_key: str
    key: str
    folders: list[str]
    missing: list[str]
    flags: tuple[str, ...]
    internaldate: object
    size: int


class _Progress:
    def __init__(self, store, job_id, kind="mail"):
        self.store = store
        self.job_id = job_id
        self.kind = kind
        self.lock = threading.Lock()
        self.values = dict(total=0, already=0, copied=0, skipped=0, failed=0, bytes=0)
        self.scopes = {}
        self.message = "Connecting"
        self.last_flush = 0.0

    def set_message(self, message):
        with self.lock:
            self.message = message
        self.flush()

    def set_scopes(self, scopes):
        with self.lock:
            self.scopes = scopes
        self.flush(force=True)

    def update_scope(self, name, **changes):
        with self.lock:
            row = self.scopes.setdefault(
                name, dict(total=0, already=0, copied=0, skipped=0, failed=0, status="pending")
            )
            row.update(changes)
            for key in ("total", "already", "copied", "skipped", "failed"):
                if key in changes:
                    self.values[key] += changes[key] - row.get(f"_old_{key}", 0)
                    row[f"_old_{key}"] = changes[key]
        self.flush()

    def increment(self, name, field, amount=1):
        with self.lock:
            self.values[field] += amount
            row = self.scopes.setdefault(
                name, dict(total=0, already=0, copied=0, skipped=0, failed=0, status="running")
            )
            row[field] = row.get(field, 0) + amount
        self.flush()

    def total(self, name, amount):
        with self.lock:
            old = self.scopes.setdefault(
                name, dict(total=0, already=0, copied=0, skipped=0, failed=0, status="pending")
            ).get("total", 0)
            self.values["total"] += amount - old
            self.scopes[name]["total"] = amount
        self.flush()

    def flush(self, force=False):
        now = time.monotonic()
        with self.lock:
            if not force and now - self.last_flush < FLUSH_SECONDS:
                return
            self.last_flush = now
            values = dict(self.values)
            scopes = {
                k: {field: val for field, val in v.items() if not field.startswith("_old_")}
                for k, v in self.scopes.items()
            }
            message = self.message
        self.store.update_task(self.job_id, self.kind, **values, message=message)
        self.store.save_scopes(self.job_id, self.kind, scopes)


class MailMigration:
    def __init__(
        self,
        store,
        job_id: int,
        config: JobConfig,
        control: RunControl,
        source_factory=make_source,
        dest_factory=make_dest,
        log=None,
    ):
        self.store = store
        self.job_id = job_id
        self.config = config
        self.control = control
        self.source_factory = source_factory
        self.dest_factory = dest_factory
        self.log = log or self._log
        self.options = config.options
        self.progress = _Progress(store, job_id)
        self.index = DestIndex(scope=self.options.dedup_scope)
        self._failed = 0
        self._failure_lock = threading.Lock()
        self._queue = queue.Queue(maxsize=max(2, self.options.workers * 2))
        self._coordinator_source = None
        self._coordinator_dest = None

    def _log(self, level, message):
        self.store.add_event(self.job_id, level, message)

    def _say(self, message, level="info"):
        self.log(level, message)

    def _note_failure(self, scope_name, source_key, key, folders, error):
        text = str(error)[:1000]
        self.store.ledger_mark(
            self.job_id, "mail", source_key, db.FAILED, match_key=key, targets=folders, error=text
        )
        self.progress.increment(scope_name, "failed")
        with self._failure_lock:
            self._failed += 1

    def _retry(self, operation, connection=None):
        def waiting(message):
            self.progress.set_message(message)

        return retry_call(
            operation,
            self.control,
            connection=connection,
            on_wait=waiting,
            on_retry=lambda attempt, exc: self._say(
                f"Temporary server error; retry {attempt}: {exc}", "warning"
            ),
        )

    def _connect(self, obj):
        return self._retry(obj.connect, obj)

    def _index_folder(self, dest, folder):
        self._retry(lambda: dest.index_folder(folder, self.index), dest)

    def _resolve_targets(self, dest, targets):
        names = []
        for target in targets:
            if self.control.stopped or self.control.aborted:
                break
            name, _exists = self._retry(
                lambda target=target: dest.resolve(target, create=not self.options.dry_run), dest
            )
            if name is not None and name not in names:
                names.append(name)
        return names

    def _enqueue(self, batch):
        while not self.control.stopped and not self.control.aborted:
            try:
                self._queue.put(batch, timeout=0.2)
                return True
            except queue.Full:
                continue
        return False

    def _scan_scope(self, src, dest, scope):
        if self.control.stopped or self.control.aborted:
            return
        self.progress.set_message(f"Scanning {scope.name}")
        self.progress.update_scope(scope.name, status="running")
        validity, uids = self._retry(lambda: src.open_scope(scope), src)
        if validity is None:
            return
        self.progress.total(scope.name, len(uids))
        source_keys = [f"{scope.name}|{validity}|{uid}" for uid in uids]
        rows = self.store.ledger_get_many(self.job_id, "mail", source_keys)
        work = []
        work_bytes = 0
        for uid_chunk_start in range(0, len(uids), META_BATCH_SIZE):
            if self.control.stopped or self.control.aborted:
                break
            chunk = uids[uid_chunk_start : uid_chunk_start + META_BATCH_SIZE]
            metadata_uids = []
            for uid in chunk:
                row = rows.get(f"{scope.name}|{validity}|{uid}")
                if row and row["status"] in db.SETTLED:
                    targets = row.get("targets") or []
                    for target in targets:
                        self._index_folder(dest, target)
                    self.index.account_for(row.get("match_key") or "", targets)
                    self.progress.increment(scope.name, "already")
                else:
                    metadata_uids.append(uid)
            if not metadata_uids:
                continue
            metas = self._retry(lambda uids=metadata_uids: src.fetch_meta(scope, uids), src) or {}
            for uid in metadata_uids:
                if self.control.stopped or self.control.aborted:
                    break
                meta = metas.get(uid)
                source_key = f"{scope.name}|{validity}|{uid}"
                if meta is None:
                    self._note_failure(scope.name, source_key, None, None, "message vanished from source")
                    continue
                if meta.deleted and self.options.skip_deleted:
                    self.progress.increment(scope.name, "skipped")
                    continue
                if not meta.targets:
                    continue
                folder_keys = self._resolve_targets(dest, meta.targets)
                if self.options.dedup_scope == "folder" and getattr(dest, "index_mode", "lazy") != "all":
                    for folder in folder_keys:
                        self._index_folder(dest, folder)
                missing = self.index.decide(meta.key, folder_keys)
                if not missing:
                    self.store.ledger_mark(
                        self.job_id, "mail", source_key, db.SKIPPED, match_key=meta.key, targets=folder_keys
                    )
                    self.progress.increment(scope.name, "skipped")
                    continue
                if self.options.dry_run:
                    self.progress.increment(scope.name, "copied")
                    continue
                item = _WorkItem(
                    scope,
                    uid,
                    source_key,
                    meta.key,
                    folder_keys,
                    missing,
                    tuple(meta.flags),
                    meta.internaldate,
                    meta.size,
                )
                if work and (len(work) >= MAX_BATCH_MESSAGES or work_bytes + item.size > MAX_BATCH_BYTES):
                    if not self._enqueue(work):
                        return
                    work, work_bytes = [], 0
                work.append(item)
                work_bytes += max(0, item.size)
        if work and not self.control.stopped and not self.control.aborted:
            self._enqueue(work)
        self.progress.update_scope(scope.name, status="done" if not self.control.stopped else "paused")

    def _worker(self, worker_id):
        src = self.source_factory(self.config.endpoints["mail"]["source"], self.options)
        dest = self.dest_factory(self.config.endpoints["mail"]["dest"], self.options)
        connected = False
        try:
            while not self.control.stopped and not self.control.aborted:
                try:
                    batch = self._queue.get(timeout=0.2)
                except queue.Empty:
                    if getattr(self, "_producer_done", False):
                        break
                    continue
                if batch is None:
                    self._queue.task_done()
                    break
                if self.control.stopped or self.control.aborted:
                    self._queue.task_done()
                    continue
                try:
                    if not connected:
                        self._connect(src)
                        self._connect(dest)
                        connected = True
                    self.progress.set_message("Copying")
                    self._copy_batch(src, dest, batch)
                except (AuthError, FatalError) as exc:
                    self.control.abort(exc)
                except BaseException as exc:
                    error = wrap(exc)
                    if isinstance(error, (AuthError, FatalError)):
                        self.control.abort(error)
                    else:
                        for item in batch:
                            self._note_failure(
                                item.scope.name, item.source_key, item.key, item.folders, error
                            )
                finally:
                    self._queue.task_done()
        finally:
            for obj in (src, dest):
                try:
                    obj.close()
                except Exception:
                    pass

    def _copy_batch(self, src, dest, batch):
        scope = batch[0].scope
        uids = [item.uid for item in batch]
        try:
            bodies = self._retry(lambda: src.fetch_bodies(scope, uids), src) or {}
        except (AuthError, FatalError):
            raise
        except BaseException as exc:
            error = wrap(exc)
            for item in batch:
                self._note_failure(item.scope.name, item.source_key, item.key, item.folders, error)
            return
        for item in batch:
            if self.control.stopped or self.control.aborted:
                return
            raw = bodies.get(item.uid)
            if raw is None:
                self._note_failure(
                    item.scope.name, item.source_key, item.key, item.folders, "message vanished from source"
                )
                continue
            self.store.ledger_mark(
                self.job_id,
                "mail",
                item.source_key,
                db.UPLOADING,
                match_key=item.key,
                targets=item.folders,
                count_attempt=True,
            )
            try:
                ref = self._retry(
                    lambda item=item, raw=raw: dest.upload(
                        raw, item.missing, item.flags, item.internaldate, item.key, self.index
                    ),
                    dest,
                )
                if self.control.stopped or self.control.aborted:
                    return
                self.store.ledger_mark(
                    self.job_id,
                    "mail",
                    item.source_key,
                    db.DONE,
                    match_key=item.key,
                    targets=item.folders,
                    dest_ref=ref,
                )
                self.progress.increment(item.scope.name, "copied")
                self.progress.increment(item.scope.name, "bytes", len(raw))
            except (AuthError, FatalError) as exc:
                self.control.abort(exc)
                return
            except BaseException as exc:
                self._note_failure(item.scope.name, item.source_key, item.key, item.folders, wrap(exc))

    def _verify(self):
        self.progress.set_message("Verifying destination")
        endpoint = self.config.endpoints["mail"]["dest"]
        dest = self.dest_factory(endpoint, self.options)
        try:
            self._connect(dest)
            index = DestIndex(scope=self.options.dedup_scope)
            rows = self.store.ledger_items(self.job_id, "mail", limit=None)
            targets = {target for row in rows if row["status"] in db.SETTLED for target in row["targets"]}
            if getattr(dest, "index_mode", "lazy") == "all" or self.options.dedup_scope == "account":
                self._retry(lambda: dest.index_all(index), dest)
            else:
                for target in targets:
                    self._retry(lambda target=target: dest.index_folder(target, index), dest)
            missing_count = 0
            for row in rows:
                if row["status"] not in db.SETTLED:
                    continue
                missing = index.decide(row.get("match_key") or "", row["targets"])
                if missing:
                    self.store.ledger_mark(
                        self.job_id,
                        "mail",
                        row["source_key"],
                        db.FAILED,
                        error="missing in destination after copy (verify)",
                    )
                    missing_count += 1
            if missing_count:
                self._say(
                    f"Verification marked {missing_count} missing destination item(s) failed", "warning"
                )
                with self._failure_lock:
                    self._failed += missing_count
        finally:
            dest.close()

    def run(self) -> TaskResult:
        self.store.reset_task(self.job_id, "mail", "running")
        self.progress.flush(force=True)
        self._coordinator_source = self.source_factory(self.config.endpoints["mail"]["source"], self.options)
        self._coordinator_dest = self.dest_factory(self.config.endpoints["mail"]["dest"], self.options)
        workers = []
        try:
            self.progress.set_message("Connecting to source and destination")
            self._connect(self._coordinator_source)
            self._connect(self._coordinator_dest)
            scopes = self._retry(self._coordinator_source.scopes, self._coordinator_source) or []
            self.progress.set_scopes(
                {
                    scope.name: dict(total=0, already=0, copied=0, skipped=0, failed=0, status="pending")
                    for scope in scopes
                }
            )
            if (
                getattr(self._coordinator_dest, "index_mode", "lazy") == "all"
                or self.options.dedup_scope == "account"
            ):
                self.progress.set_message("Indexing destination…")
                self._retry(lambda: self._coordinator_dest.index_all(self.index), self._coordinator_dest)
            if not self.options.dry_run:
                self._producer_done = False
                for worker_id in range(self.options.workers):
                    thread = threading.Thread(
                        target=self._worker,
                        args=(worker_id,),
                        daemon=True,
                        name=f"mail-worker-{self.job_id}-{worker_id}",
                    )
                    thread.start()
                    workers.append(thread)
            for scope in scopes:
                if self.control.stopped or self.control.aborted:
                    break
                self._scan_scope(self._coordinator_source, self._coordinator_dest, scope)
            self._producer_done = True
            if not self.options.dry_run:
                for _ in workers:
                    while not self.control.stopped and not self.control.aborted:
                        try:
                            self._queue.put(None, timeout=0.2)
                            break
                        except queue.Full:
                            continue
                for thread in workers:
                    thread.join()
            if self.control.aborted:
                error = str(self.control.error or "Task aborted")
                status = "failed"
            elif self.control.stopped:
                error = None
                status = "paused" if self.control.reason == "pause" else "cancelled"
            else:
                error = None
                if self.options.verify and not self.options.dry_run:
                    self._verify()
                with self._failure_lock:
                    status = "completed_with_errors" if self._failed else "completed"
            self.progress.set_message(status.replace("_", " ").capitalize())
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id,
                "mail",
                status=status,
                message=error or self.progress.message,
                finished_at=time.time(),
            )
            return TaskResult(status, error)
        except (AuthError, FatalError) as exc:
            error = str(exc)
            self._say(error, "error")
            self.control.abort(exc)
            for thread in workers:
                thread.join(timeout=1)
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id, "mail", status="failed", message=error, finished_at=time.time()
            )
            return TaskResult("failed", error)
        except BaseException as exc:
            error = str(wrap(exc))
            self._say(error, "error")
            self.progress.flush(force=True)
            self.store.update_task(
                self.job_id, "mail", status="failed", message=error, finished_at=time.time()
            )
            return TaskResult("failed", error)
        finally:
            self._producer_done = True
            for obj in (self._coordinator_source, self._coordinator_dest):
                try:
                    obj.close()
                except Exception:
                    pass
