"""Background job lifecycle and scheduling."""

import shutil
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from emailsync.accounts import check_account_compat, load_accounts, resolve_endpoint
from emailsync.config import Settings
from emailsync.dav.pipeline import DavMigration
from emailsync.jobs.control import RunControl, TaskResult
from emailsync.mail.pipeline import MailMigration
from emailsync.oauth import TokenManager
from emailsync.options import JobConfig

_ACTIVE = {"queued", "running"}
_RESUMABLE = {
    "paused",
    "cancelled",
    "failed",
    "completed",
    "completed_with_errors",
    "interrupted",
}


class ResetConfirmationRequired(ValueError):
    def __init__(self, kinds):
        self.kinds = list(kinds)
        super().__init__(
            "Changing the source or destination clears copy records for: " + ", ".join(self.kinds)
        )


def _same_server(new, old):
    return (
        new.type == old.type
        and new.host.lower() == old.host.lower()
        and new.url.rstrip("/").lower() == old.url.rstrip("/").lower()
        and new.username.lower() == old.username.lower()
    )


def _same_server_ignoring_username(new, old):
    return (
        new.type == old.type
        and new.host.lower() == old.host.lower()
        and new.url.rstrip("/").lower() == old.url.rstrip("/").lower()
    )


def merge_saved_settings(config: JobConfig, old: JobConfig) -> list[str]:
    errors = []
    ordered_kinds = sorted(config.endpoints, key=lambda kind: kind != "mail")
    for kind in ordered_kinds:
        pair = config.endpoints[kind]
        old_pair = old.endpoints.get(kind) or {}
        for side, endpoint in pair.items():
            previous = old_pair.get(side)
            if endpoint is None or previous is None or endpoint.account_id is not None:
                continue
            if endpoint.type == "file":
                if not endpoint.files:
                    endpoint.files = list(previous.files)
                continue
            if kind != "mail" and (endpoint.password or endpoint.token):
                continue
            if kind != "mail":
                previous_mail = (old.endpoints.get("mail") or {}).get(side)
                current_mail = (config.endpoints.get("mail") or {}).get(side)
                inherited_credentials = (
                    previous_mail is not None
                    and previous.password == previous_mail.password
                    and previous.token == previous_mail.token
                    and previous.username == previous_mail.username
                )
                current_mail_has_credentials = current_mail and (current_mail.password or current_mail.token)
                if (
                    inherited_credentials
                    and current_mail_has_credentials
                    and _same_server_ignoring_username(endpoint, previous)
                    and (not endpoint.username or endpoint.username == previous.username)
                ):
                    endpoint.username = current_mail.username
                    endpoint.password = current_mail.password
                    endpoint.token = current_mail.token
                    continue
            if endpoint.password or endpoint.token or not (previous.password or previous.token):
                continue
            if _same_server(endpoint, previous):
                endpoint.password = previous.password
                endpoint.token = previous.token
            else:
                errors.append(
                    f"{kind.capitalize()} {side}: enter the password again because the server or username changed."
                )
    return errors


def _identity(endpoint):
    if endpoint is None:
        return None
    if endpoint.type == "file":
        return ("file",)
    return (
        endpoint.type,
        endpoint.account_id,
        endpoint.host.lower(),
        endpoint.effective_port() if endpoint.type in ("imap", "gmail") else 0,
        endpoint.username.lower(),
        endpoint.url.rstrip("/").lower(),
    )


class JobManager:
    def __init__(
        self, store, secretbox, settings: Settings, mail_factory=MailMigration, dav_factory=DavMigration
    ):
        self.store = store
        self.secretbox = secretbox
        self.settings = settings
        self.token_manager = TokenManager(store, secretbox)
        self.mail_factory = mail_factory
        self.dav_factory = dav_factory
        self._lock = threading.RLock()
        self._controls = {}
        self._threads = {}
        self._stop = threading.Event()
        self._scheduler = None

    def start(self):
        with self._lock:
            if self._scheduler and self._scheduler.is_alive():
                return
            self._stop.clear()
            for job in self.store.list_jobs():
                if job["status"] in _ACTIVE:
                    if self.settings.auto_resume:
                        self.store.update_job(job["id"], status="queued", message="Resuming after restart")
                        self.store.add_event(job["id"], "info", "Resuming after restart")
                    else:
                        self.store.update_job(job["id"], status="interrupted", finished_at=time.time())
                        self.store.add_event(job["id"], "warning", "Interrupted by application restart")
            self._scheduler = threading.Thread(target=self._scheduler_loop, daemon=True, name="job-scheduler")
            self._scheduler.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            controls = list(self._controls.values())
            scheduler = self._scheduler
            threads = list(self._threads.values())
        for control in controls:
            control.stop("pause")
        if scheduler and scheduler is not threading.current_thread():
            scheduler.join(timeout=10)
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=30)

    def create_job(self, config: JobConfig) -> int:
        errors = self._validate(config)
        if errors:
            raise ValueError(" ".join(errors))
        public, secrets = config.split()
        encrypted = self.secretbox.encrypt(secrets) if secrets else None
        job_id = self.store.create_job(config.name.strip(), public, encrypted)
        self.store.add_event(job_id, "info", "Job created")
        return job_id

    def _validate(self, config: JobConfig) -> list[str]:
        errors = config.validate()
        accounts = {int(account["id"]): account for account in self.store.list_accounts()}
        for pair in config.endpoints.values():
            for endpoint in pair.values():
                if endpoint and endpoint.account_id is not None:
                    account = accounts.get(int(endpoint.account_id))
                    if account is None:
                        errors.append(f"Saved account {endpoint.account_id} no longer exists.")
                    elif error := check_account_compat(endpoint, account):
                        errors.append(error)
        return errors

    def _upload_dirs(self, config):
        upload_dirs = set()
        upload_root = self.settings.uploads_dir.resolve()
        for pair in config.endpoints.values():
            for endpoint in pair.values():
                if endpoint:
                    for filename in endpoint.files:
                        parent = Path(filename).resolve().parent
                        try:
                            parent.relative_to(upload_root)
                        except ValueError:
                            continue
                        upload_dirs.add(parent)
        return upload_dirs

    def _other_job_upload_dirs(self, job_id):
        upload_dirs = set()
        for other in self.store.list_jobs():
            if other["id"] != job_id:
                upload_dirs.update(self._upload_dirs(self._load_config(other, resolve=False)))
        return upload_dirs

    def edit_job(self, job_id: int, config: JobConfig, *, confirm_reset: bool = False) -> list[str]:
        with self._lock:
            job = self.store.get_job(job_id)
            if not job:
                raise KeyError(job_id)
            thread = self._threads.get(job_id)
            if job["status"] in _ACTIVE or (thread and thread.is_alive()):
                raise ValueError("Pause the migration or wait for it to finish before editing it.")

            old = self._load_config(job, resolve=False)
            config.options.dav_match_fingerprint = old.options.dav_match_fingerprint
            errors = [*merge_saved_settings(config, old), *self._validate(config)]
            if errors:
                raise ValueError(" ".join(errors))

            changed_kinds = [
                kind
                for kind, pair in config.endpoints.items()
                if any(
                    _identity(pair.get(side)) != _identity(old.endpoints[kind].get(side))
                    for side in ("source", "dest")
                )
                and self.store.ledger_counts(job_id, kind)
            ]
            if changed_kinds and not confirm_reset:
                raise ResetConfirmationRequired(changed_kinds)

            public, secrets = config.split()
            encrypted = self.secretbox.encrypt(secrets) if secrets else None
            self.store.update_job(job_id, name=config.name.strip(), config=public, secrets_enc=encrypted)
            for kind in changed_kinds:
                self.store.ledger_reset(job_id, kind)
                self.store.add_event(
                    job_id,
                    "info",
                    f"Copy records for {kind} cleared because its source or destination changed",
                )
            unused_upload_dirs = (
                self._upload_dirs(old) - self._upload_dirs(config) - self._other_job_upload_dirs(job_id)
            )
            for directory in unused_upload_dirs:
                shutil.rmtree(directory, ignore_errors=True)
            self.store.add_event(job_id, "info", "Settings updated")
            return changed_kinds

    def resume(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        if job["status"] not in _RESUMABLE:
            return False
        self.store.update_job(
            job_id, status="queued", message=None, last_error=None, finished_at=None, next_run_at=None
        )
        self.store.add_event(job_id, "info", "Job queued")
        return True

    def run_for_real(self, job_id: int):
        with self._lock:
            job = self.store.get_job(job_id)
            if not job:
                raise KeyError(job_id)
            thread = self._threads.get(job_id)
            if job["status"] in _ACTIVE or (thread and thread.is_alive()):
                raise ValueError("Pause the migration or wait for it to finish before editing it.")
            config = job["config"]
            options = config.setdefault("options", {})
            if not options.get("dry_run"):
                raise ValueError("This migration is not a dry run.")
            options["dry_run"] = False
            self.store.update_job(job_id, config=config)
            self.store.add_event(job_id, "info", "Dry run turned off")
            return self.resume(job_id)

    def pause(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        if job["status"] == "running":
            with self._lock:
                control = self._controls.get(job_id)
            if control:
                control.stop("pause")
            return True
        if job["status"] == "queued":
            self.store.update_job(job_id, status="paused", finished_at=time.time())
            self.store.add_event(job_id, "info", "Job paused before starting")
            return True
        return False

    def cancel(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        if job["status"] == "running":
            with self._lock:
                control = self._controls.get(job_id)
            if control:
                control.stop("cancel")
            return True
        if job["status"] == "queued":
            self.store.update_job(job_id, status="cancelled", finished_at=time.time(), next_run_at=None)
            self.store.add_event(job_id, "info", "Job cancelled before starting")
            return True
        return False

    def delete(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            return False
        if job["status"] == "running":
            self.cancel(job_id)
            with self._lock:
                thread = self._threads.get(job_id)
            if thread and thread is not threading.current_thread():
                thread.join()
        config = self._load_config(job, resolve=False)
        upload_dirs = self._upload_dirs(config) - self._other_job_upload_dirs(job_id)
        self.store.delete_job(job_id)
        for directory in upload_dirs:
            shutil.rmtree(directory, ignore_errors=True)
        with self._lock:
            self._controls.pop(job_id, None)
            self._threads.pop(job_id, None)
        return True

    def clone(self, job_id: int) -> int:
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        config = self._load_config(job, resolve=False)
        config.name = f"{config.name} (copy)"
        return self.create_job(config)

    def forget_progress(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        if job["status"] == "running":
            raise ValueError("Cannot forget progress while the job is running.")
        self.store.ledger_reset(job_id)
        self.store.add_event(job_id, "info", "Migration progress forgotten")
        return True

    def _load_config(self, job, resolve=True) -> JobConfig:
        secrets = self.secretbox.decrypt(job.get("secrets_enc"))
        config = JobConfig.from_dict(job["config"], secrets)
        if resolve:
            accounts = load_accounts(self.store, self.secretbox)
            for pair in config.endpoints.values():
                for side, endpoint in pair.items():
                    pair[side] = resolve_endpoint(endpoint, accounts, self.token_manager)
        return config

    def _scheduler_loop(self):
        while not self._stop.is_set():
            try:
                self._schedule_once()
            except BaseException:
                for job in self.store.list_jobs():
                    if job["status"] == "queued":
                        self.store.add_event(
                            job["id"], "error", "Scheduler error: " + traceback.format_exc()[-1500:]
                        )
                self._stop.wait(3)
            self._stop.wait(3)

    def _schedule_once(self):
        now = time.time()
        for job in self.store.list_jobs():
            if job["status"] != "queued" and job.get("next_run_at") and job["next_run_at"] <= now:
                self.store.update_job(job["id"], status="queued", next_run_at=None)
                self.store.add_event(job["id"], "info", "Scheduled sync run queued")
        with self._lock:
            running = sum(1 for thread in self._threads.values() if thread.is_alive())
            available = max(0, self.settings.max_concurrent_jobs - running)
            queued = [job for job in self.store.list_jobs() if job["status"] == "queued"]
            for job in queued[:available]:
                if job["id"] in self._threads and self._threads[job["id"]].is_alive():
                    continue
                thread = threading.Thread(
                    target=self._run_job, args=(job["id"],), daemon=True, name=f"job-{job['id']}"
                )
                self._threads[job["id"]] = thread
                thread.start()

    def _log(self, job_id, level, message):
        self.store.add_event(job_id, level, message)

    def _migration(self, job_id, kind, config, control):
        def log(level, message):
            return self._log(job_id, level, message)

        if kind == "mail":
            return self.mail_factory(self.store, job_id, config, control, log=log)
        return self.dav_factory(self.store, job_id, kind, config, control, log=log)

    def _run_job(self, job_id):
        control = RunControl()
        with self._lock:
            self._controls[job_id] = control
        job = self.store.get_job(job_id)
        if not job:
            return
        started = time.time()
        self.store.update_job(
            job_id, status="running", started_at=started, finished_at=None, message="Running", last_error=None
        )
        try:
            config = self._load_config(job)
            results = {}
            kinds = config.enabled_kinds()
            with ThreadPoolExecutor(
                max_workers=max(1, len(kinds)), thread_name_prefix=f"job-{job_id}-task"
            ) as pool:
                futures = {
                    pool.submit(self._migration(job_id, kind, config, control).run): kind for kind in kinds
                }
                for future, kind in ((future, futures[future]) for future in futures):
                    try:
                        result = future.result()
                    except BaseException as exc:
                        error = str(exc)
                        self._log(job_id, "error", f"{kind} task crashed: {traceback.format_exc()[-2000:]}")
                        result = TaskResult("failed", error)
                    results[kind] = result
            statuses = [result.status for result in results.values()]
            if "cancelled" in statuses or control.reason == "cancel":
                status = "cancelled"
            elif "paused" in statuses or control.reason == "pause":
                status = "paused"
            elif "failed" in statuses:
                status = "failed"
            elif "completed_with_errors" in statuses:
                status = "completed_with_errors"
            else:
                status = "completed"
            errors = [result.error for result in results.values() if result.error]
            error_text = "; ".join(errors)[:4000] or None
            updates = dict(
                status=status,
                finished_at=time.time(),
                last_error=error_text,
                message=status.replace("_", " ").capitalize(),
                run_count=job["run_count"] + 1,
            )
            sync_interval = config.options.sync_interval_minutes
            auth_error = any(
                "auth" in (error or "").lower() or "login" in (error or "").lower() for error in errors
            )
            if (
                sync_interval
                and status not in ("paused", "cancelled")
                and not (status == "failed" and auth_error)
            ):
                updates["next_run_at"] = time.time() + sync_interval * 60
            else:
                updates["next_run_at"] = None
            self.store.update_job(job_id, **updates)
            self._log(job_id, "info", f"Job {status.replace('_', ' ')}")
        except BaseException as exc:
            error = str(exc)
            summary = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:]
            self._log(job_id, "error", f"Job crashed: {summary}")
            self.store.update_job(
                job_id,
                status="failed",
                finished_at=time.time(),
                last_error=error,
                message="Job failed",
                run_count=job["run_count"] + 1,
                next_run_at=None,
            )
        finally:
            with self._lock:
                self._controls.pop(job_id, None)

    def api_job(self, job_id: int):
        job = self.store.get_job(job_id)
        if not job:
            return None
        return {
            "id": job["id"],
            "name": job["name"],
            "status": job["status"],
            "message": job.get("message"),
            "last_error": job.get("last_error"),
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
            "next_run_at": job.get("next_run_at"),
            "run_count": job.get("run_count"),
            "config": job["config"],
            "tasks": self.store.get_tasks(job_id),
            "scopes": self.store.get_scopes(job_id),
        }
