import time

from fastapi.testclient import TestClient

from emailsync.config import Settings
from emailsync.crypto import SecretBox
from emailsync.db import Store
from emailsync.jobs.control import TaskResult
from emailsync.jobs.manager import JobManager
from emailsync.options import Endpoint, JobConfig, Options
from emailsync.web.app import create_app


class CompletedMigration:
    def __init__(self, store, job_id, *_args, **_kwargs):
        self.store = store
        self.job_id = job_id

    def run(self):
        return TaskResult("completed")


def make_manager(tmp_path):
    settings = Settings(tmp_path, "test-key", 2, True, "INFO")
    store = Store(settings.db_path)
    secretbox = SecretBox.from_settings(settings.secret_key, settings.data_dir)
    manager = JobManager(store, secretbox, settings, mail_factory=CompletedMigration)
    return settings, store, manager


def mail_config(password="super-secret"):
    return JobConfig(
        "Gmail migration",
        {
            "mail": {
                "source": Endpoint("gmail", host="imap.gmail.com", username="alice", password=password),
                "dest": Endpoint("jmap", url="https://mail.example.com", username="alice", password=password),
            },
            "contacts": {"source": None, "dest": None},
            "calendars": {"source": None, "dest": None},
        },
        Options(verify=False),
    )


def test_job_manager_encrypts_credentials_and_runs_queued_jobs(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = manager.create_job(mail_config())
    row = store.get_job(job_id)
    assert "super-secret" not in str(row["config"])
    assert row["secrets_enc"]
    manager._schedule_once()
    deadline = time.time() + 3
    while time.time() < deadline and store.get_job(job_id)["status"] == "queued":
        time.sleep(0.02)
    assert store.get_job(job_id)["status"] == "completed"
    assert manager.api_job(job_id)["config"]["endpoints"]["mail"]["source"].get("password") is None


def test_startup_recovery_marks_jobs_interrupted_when_auto_resume_disabled(tmp_path):
    settings, store, _manager = make_manager(tmp_path)
    settings = Settings(tmp_path, "test-key", 2, False, "INFO")
    manager = JobManager(store, SecretBox.from_settings(settings.secret_key, settings.data_dir), settings)
    job_id = store.create_job("job", {}, None, status="running")
    manager.start()
    manager.stop()
    assert store.get_job(job_id)["status"] == "interrupted"
    assert store.get_events(job_id)[0]["message"] == "Interrupted by application restart"


def test_startup_auto_resume_and_due_sync_schedule_jobs(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = store.create_job("job", {}, None, status="running")
    manager._scheduler_loop = lambda: None
    manager.start()
    manager.stop()
    assert store.get_job(job_id)["status"] == "queued"
    assert any(event["message"] == "Resuming after restart" for event in store.get_events(job_id))

    job_id = manager.create_job(mail_config())
    store.update_job(job_id, status="completed", next_run_at=time.time() - 1)
    manager._schedule_once()
    deadline = time.time() + 3
    while time.time() < deadline and store.get_job(job_id)["status"] not in ("completed", "failed"):
        time.sleep(0.02)
    assert store.get_job(job_id)["status"] == "completed"
    assert any(event["message"] == "Scheduled sync run queued" for event in store.get_events(job_id))


def test_web_dashboard_detail_validation_and_secret_redaction(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    app = create_app(settings, store, manager)
    with TestClient(app) as client:
        invalid = client.post("/jobs", data={"name": ""})
        assert invalid.status_code == 422
        assert "Configure a source and a destination" in invalid.text
        response = client.post(
            "/jobs",
            data={
                "name": "UI migration",
                "mail_source_type": "imap",
                "mail_source_host": "source.example",
                "mail_source_username": "alice",
                "mail_source_password": "html-secret",
                "mail_dest_type": "imap",
                "mail_dest_host": "dest.example",
                "mail_dest_username": "alice",
                "mail_dest_password": "html-secret",
                "workers": "1",
                "dedup_scope": "folder",
                "skip_deleted": "on",
                "verify": "on",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        job_id = int(response.headers["location"].split("/")[-1])
        for url in ("/", f"/jobs/{job_id}", f"/jobs/{job_id}/fragment", "/jobs/fragment"):
            page = client.get(url)
            assert page.status_code == 200
            assert "html-secret" not in page.text
        api = client.get("/api/jobs")
        assert "html-secret" not in api.text
        assert client.get("/healthz").json() == {"ok": True}


def test_contacts_jmap_destination_inherits_mail_jmap_url_and_token(tmp_path):
    class CapturingManager:
        config = None

        def start(self):
            return None

        def stop(self):
            return None

        def create_job(self, config):
            self.config = config
            return 42

    settings = Settings(tmp_path, "test-key", 2, True, "INFO")
    manager = CapturingManager()
    app = create_app(settings, Store(settings.db_path), manager)
    with TestClient(app) as client:
        response = client.post(
            "/jobs",
            data={
                "name": "Contacts migration",
                "mail_source_type": "imap",
                "mail_source_host": "imap.example.test",
                "mail_source_username": "alice",
                "mail_source_password": "source-secret",
                "mail_dest_type": "jmap",
                "mail_dest_url": "https://mail.example.test",
                "mail_dest_username": "alice",
                "mail_dest_token": "mail-token",
                "contacts_source_type": "carddav",
                "contacts_source_url": "https://contacts.example.test",
                "contacts_source_username": "alice",
                "contacts_source_password": "dav-secret",
                "contacts_dest_type": "jmap",
            },
            follow_redirects=False,
        )
    assert response.status_code == 303
    destination = manager.config.endpoints["contacts"]["dest"]
    assert destination.url == "https://mail.example.test"
    assert destination.username == "alice"
    assert destination.token == "mail-token"
    assert destination.password == ""


def test_api_test_lists_jmap_dav_destination_collections(tmp_path, monkeypatch):
    class FakeJmapDestination:
        collections = [{"displayname": "Default", "id": "address-book-1"}]

        def __init__(self, endpoint, kind):
            self.endpoint = endpoint
            self.kind = kind

        def connect(self):
            return self

        def close(self):
            return None

    monkeypatch.setattr("emailsync.web.app.JmapDavDestination", FakeJmapDestination)
    settings, store, manager = make_manager(tmp_path)
    app = create_app(settings, store, manager)
    with TestClient(app) as client:
        response = client.post(
            "/api/test",
            json={
                "kind": "contacts",
                "side": "dest",
                "endpoint": {
                    "type": "jmap",
                    "url": "https://jmap.example",
                    "username": "alice",
                },
            },
        )
    assert response.status_code == 200
    assert response.json()["folders"] == [{"source": "Default", "target": "address-book-1"}]
