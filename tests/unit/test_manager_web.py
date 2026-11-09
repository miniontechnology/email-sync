import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from emailsync import db
from emailsync.config import Settings
from emailsync.crypto import SecretBox
from emailsync.db import Store
from emailsync.jobs.control import TaskResult
from emailsync.jobs.manager import JobManager, ResetConfirmationRequired
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


def config_with_contacts(password="super-secret", dry_run=False):
    config = mail_config(password)
    config.options.dry_run = dry_run
    config.endpoints["contacts"] = {
        "source": Endpoint("carddav", url="https://contacts.source", username="alice", password=password),
        "dest": Endpoint("carddav", url="https://contacts.dest", username="alice", password=password),
    }
    return config


def completed_job(manager, store, config):
    job_id = manager.create_job(config)
    store.update_job(job_id, status="completed")
    return job_id


def edit_form_data(**overrides):
    data = {
        "name": "Edited migration",
        "mail_source_type": "gmail",
        "mail_source_host": "imap.gmail.com",
        "mail_source_port": "993",
        "mail_source_security": "ssl",
        "mail_source_username": "alice",
        "mail_dest_type": "jmap",
        "mail_dest_url": "https://mail.example.com",
        "mail_dest_username": "alice",
        "workers": "4",
        "dedup_scope": "folder",
        "archive_target": "@archive",
        "sync_interval_minutes": "0",
        "folder_map": "INBOX => Work",
        "skip_deleted": "on",
        "verify": "on",
        "sync_flags_labels": "on",
    }
    data.update(overrides)
    return data


def test_options_default_state_sync_enabled_and_stale_removal_disabled():
    options = Options.from_dict({})

    assert options.sync_flags_labels is True
    assert options.remove_stale_labels is False


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


def test_edit_job_refuses_queued_or_running_jobs(tmp_path):
    _settings, store, manager = make_manager(tmp_path)
    job_id = manager.create_job(mail_config())
    config = mail_config()

    for status in ("queued", "running"):
        store.update_job(job_id, status=status)
        with pytest.raises(ValueError, match="Pause the migration or wait for it to finish"):
            manager.edit_job(job_id, config)

    class LiveThread:
        def is_alive(self):
            return True

    store.update_job(job_id, status="completed")
    manager._threads[job_id] = LiveThread()
    with pytest.raises(ValueError, match="Pause the migration or wait for it to finish"):
        manager.edit_job(job_id, config)


def test_edit_job_keeps_blank_password_for_same_server_and_rejects_changed_server(tmp_path):
    _settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())

    assert manager.edit_job(job_id, mail_config(password="")) == []
    saved = manager._load_config(store.get_job(job_id), resolve=False)
    assert saved.endpoints["mail"]["source"].password == "super-secret"
    assert saved.endpoints["mail"]["dest"].password == "super-secret"

    changed = mail_config(password="")
    changed.endpoints["mail"]["source"].host = "different.example"
    with pytest.raises(ValueError, match="enter the password again because the server or username changed"):
        manager.edit_job(job_id, changed)


def test_options_only_edit_keeps_ledger(tmp_path):
    _settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    store.ledger_mark(job_id, "mail", "message-1", db.DONE)

    changed = manager._load_config(store.get_job(job_id), resolve=False)
    changed.options.dry_run = True
    changed.options.folder_map = {"INBOX": "Work"}

    assert manager.edit_job(job_id, changed) == []
    assert store.ledger_counts(job_id, "mail") == {db.DONE: 1}


def test_edit_job_requires_confirmation_and_resets_only_changed_kind(tmp_path):
    _settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, config_with_contacts())
    store.ledger_mark(job_id, "mail", "message-1", db.DONE)
    store.ledger_mark(job_id, "contacts", "contact-1", db.SKIPPED)
    changed = manager._load_config(store.get_job(job_id), resolve=False)
    changed.endpoints["mail"]["dest"].url = "https://other.example"
    changed.endpoints["mail"]["dest"].password = "new-secret"

    with pytest.raises(ResetConfirmationRequired) as error:
        manager.edit_job(job_id, changed)
    assert error.value.kinds == ["mail"]
    assert store.ledger_counts(job_id, "mail") == {db.DONE: 1}

    assert manager.edit_job(job_id, changed, confirm_reset=True) == ["mail"]
    assert store.ledger_counts(job_id, "mail") == {}
    assert store.ledger_counts(job_id, "contacts") == {db.SKIPPED: 1}
    assert any(
        event["message"] == "Copy records for mail cleared because its source or destination changed"
        for event in store.get_events(job_id)
    )


def test_edit_file_endpoint_keeps_or_replaces_uploads_safely(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    old_dir = settings.uploads_dir / "old"
    old_dir.mkdir(parents=True)
    old_file = old_dir / "contacts.vcf"
    old_file.write_text("old")
    config = JobConfig(
        "File migration",
        {
            "mail": {"source": None, "dest": None},
            "contacts": {
                "source": Endpoint("file", files=[str(old_file)]),
                "dest": Endpoint("carddav", url="https://contacts.dest", username="alice", password="secret"),
            },
            "calendars": {"source": None, "dest": None},
        },
        Options(verify=False),
    )
    job_id = completed_job(manager, store, config)

    without_upload = manager._load_config(store.get_job(job_id), resolve=False)
    without_upload.endpoints["contacts"]["source"].files = []
    manager.edit_job(job_id, without_upload)
    assert old_file.exists()
    assert manager._load_config(store.get_job(job_id), resolve=False).endpoints["contacts"][
        "source"
    ].files == [str(old_file)]

    new_dir = settings.uploads_dir / "new"
    new_dir.mkdir(parents=True)
    new_file = new_dir / "replacement.vcf"
    new_file.write_text("new")
    replacement = manager._load_config(store.get_job(job_id), resolve=False)
    replacement.endpoints["contacts"]["source"].files = [str(new_file)]
    manager.edit_job(job_id, replacement)
    assert not old_dir.exists()
    assert new_file.exists()


def test_editing_and_deleting_clone_preserves_original_uploads(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    old_dir = settings.uploads_dir / "shared"
    old_dir.mkdir(parents=True)
    old_file = old_dir / "contacts.vcf"
    old_file.write_text("original")
    config = JobConfig(
        "File migration",
        {
            "mail": {"source": None, "dest": None},
            "contacts": {
                "source": Endpoint("file", files=[str(old_file)]),
                "dest": Endpoint("carddav", url="https://contacts.dest", username="alice", password="secret"),
            },
            "calendars": {"source": None, "dest": None},
        },
        Options(verify=False),
    )
    original_id = completed_job(manager, store, config)
    clone_id = manager.clone(original_id)
    store.update_job(clone_id, status="paused")

    new_dir = settings.uploads_dir / "clone"
    new_dir.mkdir(parents=True)
    new_file = new_dir / "replacement.vcf"
    new_file.write_text("clone")
    clone_config = manager._load_config(store.get_job(clone_id), resolve=False)
    clone_config.endpoints["contacts"]["source"].files = [str(new_file)]
    manager.edit_job(clone_id, clone_config)

    assert old_file.exists()
    assert new_file.exists()
    assert manager.delete(clone_id)
    assert old_file.exists()
    assert not new_dir.exists()


def test_run_for_real_flips_dry_run_and_queues_job(tmp_path):
    _settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    stored = store.get_job(job_id)
    stored["config"]["options"]["dry_run"] = True
    store.update_job(job_id, config=stored["config"])
    secrets_enc = stored["secrets_enc"]

    assert manager.run_for_real(job_id)
    assert store.get_job(job_id)["status"] == "queued"
    assert store.get_job(job_id)["config"]["options"]["dry_run"] is False
    assert store.get_job(job_id)["secrets_enc"] == secrets_enc
    assert any(event["message"] == "Dry run turned off" for event in store.get_events(job_id))

    other = completed_job(manager, store, mail_config())
    with pytest.raises(ValueError, match="not a dry run"):
        manager.run_for_real(other)


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
        form = client.get("/jobs/new")
        assert form.status_code == 200
        assert 'name="sync_flags_labels" checked' in form.text
        assert 'name="remove_stale_labels" checked' not in form.text

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
                "sync_flags_labels": "on",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        job_id = int(response.headers["location"].split("/")[-1])
        options = store.get_job(job_id)["config"]["options"]
        assert options["sync_flags_labels"] is True
        assert options["remove_stale_labels"] is False
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


def test_edit_page_prefills_settings_and_never_renders_secrets(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    config = mail_config()
    config.options.dry_run = True
    config.options.folder_map = {"INBOX": "Work"}
    config.endpoints["mail"]["source"].insecure_tls = True
    job_id = completed_job(manager, store, config)
    manager._schedule_once = lambda: None

    with TestClient(create_app(settings, store, manager)) as client:
        page = client.get(f"/jobs/{job_id}/edit")
        assert page.status_code == 200
        assert 'value="Gmail migration"' in page.text
        assert 'value="imap.gmail.com"' in page.text
        assert "INBOX =&gt; Work" in page.text
        assert 'name="mail_source_insecure_tls" checked' in page.text
        assert 'name="dry_run" checked' in page.text
        assert "Leave blank to keep the saved password" in page.text
        assert "super-secret" not in page.text
        assert client.get("/jobs/999/edit").status_code == 404


def test_edit_page_shows_existing_file_basenames(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    config = JobConfig(
        "File migration",
        {
            "mail": {"source": None, "dest": None},
            "contacts": {
                "source": Endpoint("file", files=["/uploads/old/people.vcf", "/uploads/old/groups.zip"]),
                "dest": Endpoint("carddav", url="https://contacts.dest", username="alice"),
            },
            "calendars": {"source": None, "dest": None},
        },
        Options(verify=False),
    )
    job_id = completed_job(manager, store, config)
    manager._schedule_once = lambda: None

    with TestClient(create_app(settings, store, manager)) as client:
        page = client.get(f"/jobs/{job_id}/edit")

    assert "Currently: people.vcf, groups.zip — choose files to replace them" in page.text


def test_edit_post_keeps_blank_password_and_save_and_run_queues_job(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    manager._schedule_once = lambda: None

    with TestClient(create_app(settings, store, manager)) as client:
        saved = client.post(f"/jobs/{job_id}/edit", data=edit_form_data(), follow_redirects=False)
        assert saved.status_code == 303
        loaded = manager._load_config(store.get_job(job_id), resolve=False)
        assert loaded.endpoints["mail"]["source"].password == "super-secret"
        assert loaded.endpoints["mail"]["dest"].password == "super-secret"

        queued = client.post(
            f"/jobs/{job_id}/edit",
            data=edit_form_data(then_run="1"),
            follow_redirects=False,
        )
        assert queued.status_code == 303
        assert store.get_job(job_id)["status"] == "queued"


def test_edit_preserves_options_not_in_the_form(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    original = mail_config()
    original.options.dav_match_fingerprint = False
    job_id = completed_job(manager, store, original)
    manager._schedule_once = lambda: None
    edited = mail_config()
    edited.options.dav_match_fingerprint = True

    manager.edit_job(job_id, edited)

    saved = manager._load_config(store.get_job(job_id), resolve=False)
    assert saved.options.dav_match_fingerprint is False


def test_edit_preserves_separate_dav_password_and_updates_inherited_password(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    separate = config_with_contacts("mail-old")
    separate.endpoints["contacts"]["dest"].password = "carddav-own"
    separate_id = completed_job(manager, store, separate)
    inherited_id = completed_job(manager, store, config_with_contacts("mail-old"))
    manager._schedule_once = lambda: None
    app = create_app(settings, store, manager)
    form = edit_form_data(
        mail_dest_password="mail-new",
        contacts_source_type="carddav",
        contacts_source_url="https://contacts.source",
        contacts_source_username="alice",
        contacts_dest_type="carddav",
        contacts_dest_url="https://contacts.dest",
        contacts_dest_username="alice",
    )

    with TestClient(app) as client:
        for job_id in (separate_id, inherited_id):
            response = client.post(f"/jobs/{job_id}/edit", data=form, follow_redirects=False)
            assert response.status_code == 303

    separate_saved = manager._load_config(store.get_job(separate_id), resolve=False)
    inherited_saved = manager._load_config(store.get_job(inherited_id), resolve=False)
    assert separate_saved.endpoints["contacts"]["dest"].password == "carddav-own"
    assert inherited_saved.endpoints["contacts"]["dest"].password == "mail-new"


def test_edit_reset_warning_requires_confirmation_then_clears_records(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    store.ledger_mark(job_id, "mail", "message-1", db.DONE)
    manager._schedule_once = lambda: None
    data = edit_form_data(mail_dest_url="https://new-mail.example", mail_dest_password="new-secret")

    app = create_app(settings, store, manager)
    with TestClient(app) as client:
        warning = client.post(f"/jobs/{job_id}/edit", data=data)
        assert warning.status_code == 200
        assert "Confirm migration edit" in warning.text
        assert "Changing the source or destination clears the copy records for: mail." in warning.text
        assert "new-secret" not in warning.text
        token = next(iter(app.state.pending_edits))
        assert token in warning.text

        confirmed = client.post(
            f"/jobs/{job_id}/edit/confirm",
            data={"token": token},
            follow_redirects=False,
        )
        assert confirmed.status_code == 303
    assert store.ledger_counts(job_id, "mail") == {}
    assert (
        manager._load_config(store.get_job(job_id), resolve=False).endpoints["mail"]["dest"].password
        == "new-secret"
    )


def test_job_page_edit_dry_run_badge_and_run_for_real(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    dry_config = mail_config()
    dry_config.options.dry_run = True
    dry_job = completed_job(manager, store, dry_config)
    running_job = completed_job(manager, store, mail_config("other-secret"))
    regular_job = completed_job(manager, store, mail_config("regular-secret"))
    manager._schedule_once = lambda: None

    with TestClient(create_app(settings, store, manager)) as client:
        dashboard = client.get("/")
        detail = client.get(f"/jobs/{dry_job}")
        assert "Dry run" in dashboard.text
        assert "Dry run" in detail.text
        assert f'href="/jobs/{dry_job}/edit"' in detail.text
        assert f'action="/jobs/{dry_job}/run-real"' in detail.text

        run_response = client.post(f"/jobs/{dry_job}/run-real", follow_redirects=False)
        assert run_response.status_code == 303
        assert store.get_job(dry_job)["status"] == "queued"
        assert store.get_job(dry_job)["config"]["options"]["dry_run"] is False
        assert client.post(f"/jobs/{regular_job}/run-real").status_code == 400

        store.update_job(running_job, status="running")
        running_page = client.get(f"/jobs/{running_job}")
        assert f'href="/jobs/{running_job}/edit"' not in running_page.text
        assert client.get(f"/jobs/{running_job}/edit", follow_redirects=False).status_code == 303


def test_create_error_preserves_submitted_values_without_secrets(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    manager._schedule_once = lambda: None

    def fail_create(_config):
        raise ValueError("Expected account validation failure")

    manager.create_job = fail_create
    data = edit_form_data(mail_source_host="submitted.example", mail_source_password="not-rendered")

    with TestClient(create_app(settings, store, manager)) as client:
        response = client.post("/jobs", data=data)

    assert response.status_code == 422
    assert 'value="submitted.example"' in response.text
    assert "not-rendered" not in response.text


def test_edit_error_preserves_submitted_values_without_secrets(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    manager._schedule_once = lambda: None

    def fail_edit(_job_id, _config, **_kwargs):
        raise ValueError("Expected edit validation failure")

    manager.edit_job = fail_edit
    data = edit_form_data(mail_source_host="submitted-edit.example", mail_source_password="not-rendered")

    with TestClient(create_app(settings, store, manager)) as client:
        response = client.post(f"/jobs/{job_id}/edit", data=data)

    assert response.status_code == 422
    assert 'value="submitted-edit.example"' in response.text
    assert "not-rendered" not in response.text


def test_edit_confirmation_preserves_new_credentials_and_uploads(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    old_dir = settings.uploads_dir / "old"
    old_dir.mkdir(parents=True)
    old_file = old_dir / "people.vcf"
    old_file.write_text("old")
    config = config_with_contacts()
    config.endpoints["contacts"]["source"] = Endpoint("file", files=[str(old_file)])
    job_id = completed_job(manager, store, config)
    store.ledger_mark(job_id, "contacts", "contact-1", db.DONE)
    manager._schedule_once = lambda: None
    data = edit_form_data(
        contacts_source_type="file",
        contacts_dest_type="carddav",
        contacts_dest_url="https://new-contacts.example",
        contacts_dest_username="alice",
        contacts_dest_password="new-secret",
        then_run="1",
    )
    app = create_app(settings, store, manager)

    with TestClient(app) as client:
        response = client.post(
            f"/jobs/{job_id}/edit",
            data=data,
            files={"contacts_source_files": ("replacement.vcf", b"new", "text/vcard")},
        )
        assert response.status_code == 200
        assert "new-secret" not in response.text
        assert "replacement.vcf" not in response.text
        token = next(iter(app.state.pending_edits))
        pending = app.state.pending_edits[token]
        assert pending.config.endpoints["contacts"]["dest"].password == "new-secret"
        new_file = Path(pending.config.endpoints["contacts"]["source"].files[0])
        assert new_file.read_bytes() == b"new"
        assert new_file.parent.exists()

        confirmed = client.post(
            f"/jobs/{job_id}/edit/confirm",
            data={"token": token},
            follow_redirects=False,
        )

    assert confirmed.status_code == 303
    assert store.get_job(job_id)["status"] == "queued"
    saved = manager._load_config(store.get_job(job_id), resolve=False)
    assert saved.endpoints["contacts"]["dest"].password == "new-secret"
    assert saved.endpoints["contacts"]["source"].files == [str(new_file)]
    assert new_file.exists()
    assert not old_dir.exists()
    assert set(settings.uploads_dir.iterdir()) == {new_file.parent}


def test_unknown_or_expired_confirmation_token_does_not_save(tmp_path):
    settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    store.ledger_mark(job_id, "mail", "message-1", db.DONE)
    manager._schedule_once = lambda: None
    app = create_app(settings, store, manager)
    changed = edit_form_data(mail_dest_url="https://new-mail.example", mail_dest_password="new-secret")

    with TestClient(app) as client:
        unknown = client.post(
            f"/jobs/{job_id}/edit/confirm",
            data={"token": "unknown-token"},
            follow_redirects=False,
        )
        assert unknown.status_code == 303
        assert unknown.headers["location"] == f"/jobs/{job_id}/edit"

        pending_page = client.post(f"/jobs/{job_id}/edit", data=changed)
        assert pending_page.status_code == 200
        token = next(iter(app.state.pending_edits))
        app.state.pending_edits[token].created = time.time() - 1801

        expired = client.post(
            f"/jobs/{job_id}/edit/confirm",
            data={"token": token},
            follow_redirects=False,
        )

    assert expired.status_code == 303
    assert expired.headers["location"] == f"/jobs/{job_id}/edit"
    assert app.state.pending_edits == {}
    saved = manager._load_config(store.get_job(job_id), resolve=False)
    assert saved.endpoints["mail"]["dest"].url == "https://mail.example.com"
    assert saved.endpoints["mail"]["dest"].password == "super-secret"
    assert store.ledger_counts(job_id, "mail") == {db.DONE: 1}


def test_api_test_fills_saved_password_for_matching_job_endpoint(tmp_path, monkeypatch):
    settings, store, manager = make_manager(tmp_path)
    job_id = completed_job(manager, store, mail_config())
    manager._schedule_once = lambda: None
    tested = []

    class FakeSource:
        def __init__(self, endpoint, _options):
            tested.append(endpoint)

        def connect(self):
            return self

        def scopes(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr("emailsync.web.app.make_source", FakeSource)
    with TestClient(create_app(settings, store, manager)) as client:
        response = client.post(
            "/api/test",
            json={
                "kind": "mail",
                "side": "source",
                "job_id": job_id,
                "endpoint": {
                    "type": "gmail",
                    "host": "imap.gmail.com",
                    "username": "alice",
                },
            },
        )

    assert response.status_code == 200
    assert tested[0].password == "super-secret"
