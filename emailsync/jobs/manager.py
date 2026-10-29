"""Background job lifecycle and scheduling."""

import shutil
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from emailsync.accounts import load_accounts, resolve_endpoint
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
        errors = config.validate()
        if errors:
            raise ValueError(" ".join(errors))
        public, secrets = config.split()
        encrypted = self.secretbox.encrypt(secrets) if secrets else None
        job_id = self.store.create_job(config.name.strip(), public, encrypted)
        self.store.add_event(job_id, "info", "Job created")
        return job_id

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
        upload_dirs = set()
        for pair in config.endpoints.values():
            for endpoint in pair.values():
                if endpoint:
                    for filename in endpoint.files:
                        parent = Path(filename).resolve().parent
                        try:
                            parent.relative_to(self.settings.uploads_dir.resolve())
                        except ValueError:
                            continue
                        upload_dirs.add(parent)
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
