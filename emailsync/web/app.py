"""FastAPI application and browser UI."""

import re
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import UploadFile as StarletteUploadFile

from emailsync.accounts import load_accounts, resolve_endpoint
from emailsync.config import Settings, load_settings
from emailsync.connectors.dav import DavClient
from emailsync.crypto import SecretBox
from emailsync.dav.jmap_dest import JmapDavDestination
from emailsync.dav.source import DavSource
from emailsync.db import Store
from emailsync.jobs.manager import JobManager
from emailsync.mail.dest import make_dest
from emailsync.mail.folders import parse_folder_map
from emailsync.mail.source import make_source
from emailsync.oauth import OAuthService, TokenManager
from emailsync.options import Endpoint, JobConfig, Options

PACKAGE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(PACKAGE_DIR / "templates"))


def _format_job(job):
    config = job["config"]
    summaries = []
    for kind, pair in config.get("endpoints", {}).items():
        source, dest = pair.get("source"), pair.get("dest")
        if source and dest:
            summaries.append(f"{kind}: {source.get('type', '?')} → {dest.get('type', '?')}")
    return {**job, "summary": " · ".join(summaries) or "No configured migrations"}


def _reauth_account_id(job, store):
    match = re.search(r"Sign-in for account '(.+?)' has expired or was revoked", job.get("last_error") or "")
    if not match:
        return None
    account_name = match.group(1)
    for pair in job.get("config", {}).get("endpoints", {}).values():
        for endpoint in pair.values():
            if endpoint and endpoint.get("account_id"):
                account = store.get_account(endpoint["account_id"])
                if account and account["name"] == account_name:
                    return endpoint["account_id"]
    return None


def _progress(tasks):
    for task in tasks:
        done = task["already"] + task["copied"] + task["skipped"] + task["failed"]
        task["progress"] = min(100, round(done * 100 / task["total"])) if task["total"] else 0
        elapsed = max(
            1, (task.get("finished_at") or task.get("started_at") or 0) - (task.get("started_at") or 0)
        )
        task["rate"] = round(task.get("bytes", 0) / elapsed / 1024, 1)
    return tasks


def _endpoint_from_form(form, kind, side, uploads_dir, mail_endpoint=None, upload_dirs=None):
    prefix = f"{kind}_{side}_"
    endpoint_type = str(form.get(prefix + "type") or "").strip()
    if endpoint_type in ("", "none"):
        return None
    files = []
    upload = form.getlist(prefix + "files")
    for item in upload:
        if not isinstance(item, StarletteUploadFile) or not item.filename:
            continue
        extension = Path(item.filename).suffix.lower()
        if extension not in (".vcf", ".ics", ".zip"):
            continue
        directory = uploads_dir / str(uuid.uuid4())
        directory.mkdir(parents=True, exist_ok=True)
        if upload_dirs is not None:
            upload_dirs.add(directory)
        path = directory / (Path(item.filename).name or "upload")
        with path.open("wb") as output:
            shutil.copyfileobj(item.file, output)
        files.append(str(path))
    username = str(form.get(prefix + "username") or "").strip()
    password = str(form.get(prefix + "password") or "")
    if endpoint_type in ("carddav", "caldav", "jmap") and (not username or not password):
        username = username or (mail_endpoint.username if mail_endpoint else "")
        password = password or (mail_endpoint.password if mail_endpoint else "")
    token = str(form.get(prefix + "token") or "")
    url = str(form.get(prefix + "url") or "").strip()
    account_id = form.get(prefix + "account_id")
    account_id = int(account_id) if account_id and str(account_id).isdigit() else None
    if account_id:
        username = password = token = ""
    if endpoint_type == "jmap" and mail_endpoint and mail_endpoint.type == "jmap":
        url = url or mail_endpoint.url
        if not password:
            token = token or mail_endpoint.token
    return Endpoint(
        type=endpoint_type,
        account_id=account_id,
        host=str(form.get(prefix + "host") or "").strip(),
        port=int(form.get(prefix + "port") or 0),
        security=str(form.get(prefix + "security") or "ssl"),
        username=username,
        password=password,
        token=token,
        url=url,
        insecure_tls=bool(form.get(prefix + "insecure_tls")),
        files=files,
    )


async def _build_config(request, settings):
    form = await request.form()
    upload_dir = settings.uploads_dir
    upload_dirs = set()
    endpoints = {kind: {"source": None, "dest": None} for kind in ("mail", "contacts", "calendars")}
    for kind in endpoints:
        for side in ("source", "dest"):
            mail_endpoint = endpoints["mail"][side] if kind != "mail" else None
            endpoints[kind][side] = _endpoint_from_form(
                form, kind, side, upload_dir, mail_endpoint, upload_dirs
            )
    options = Options(
        workers=int(form.get("workers") or 4),
        dedup_scope=str(form.get("dedup_scope") or "folder"),
        include_spam=bool(form.get("include_spam")),
        include_trash=bool(form.get("include_trash")),
        skip_deleted=bool(form.get("skip_deleted")),
        archive_target=str(form.get("archive_target") or ""),
        folder_map=parse_folder_map(str(form.get("folder_map") or "")),
        exclude_folders=[
            line.strip() for line in str(form.get("exclude_folders") or "").splitlines() if line.strip()
        ],
        since=str(form.get("since") or "").strip(),
        dry_run=bool(form.get("dry_run")),
        verify=bool(form.get("verify")),
        sync_interval_minutes=int(form.get("sync_interval_minutes") or 0),
    )
    config = JobConfig(
        name=str(form.get("name") or ""),
        endpoints=endpoints,
        options=options,
    )
    values = {
        key: value
        for key, value in form.items()
        if not key.endswith(("_password", "_token")) and not isinstance(value, StarletteUploadFile)
    }
    return config, values, upload_dirs


def create_app(settings: Settings | None = None, store=None, manager=None):
    settings = settings or load_settings()
    store = store or Store(settings.db_path)
    secretbox = getattr(manager, "secretbox", None) or SecretBox.from_settings(
        settings.secret_key, settings.data_dir
    )
    manager = manager or JobManager(store, secretbox, settings)
    token_manager = getattr(manager, "token_manager", None) or TokenManager(store, secretbox)
    oauth = OAuthService(store, secretbox)

    @asynccontextmanager
    async def lifespan(_app):
        manager.start()
        yield
        manager.stop()

    app = FastAPI(title="Email Sync", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.manager = manager
    app.state.secretbox = secretbox
    app.state.oauth = oauth
    app.mount("/static", StaticFiles(directory=str(PACKAGE_DIR / "static")), name="static")

    def _render_jobs(request):
        jobs = [_format_job(job) for job in store.list_jobs()]
        for job in jobs:
            job["tasks"] = _progress(store.get_tasks(job["id"]))
        return templates.TemplateResponse(request=request, name="dashboard.html", context={"jobs": jobs})

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        return _render_jobs(request)

    @app.get("/jobs/fragment", response_class=HTMLResponse)
    async def jobs_fragment(request: Request):
        jobs = [_format_job(job) for job in store.list_jobs()]
        for job in jobs:
            job["tasks"] = _progress(store.get_tasks(job["id"]))
        return templates.TemplateResponse(request=request, name="job_cards.html", context={"jobs": jobs})

    @app.get("/jobs/new", response_class=HTMLResponse)
    async def new_job(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="new_job.html",
            context={"errors": [], "values": {}, "accounts": store.list_accounts()},
        )

    @app.post("/jobs", response_class=HTMLResponse)
    async def create_job(request: Request):
        upload_dirs = set()
        try:
            config, values, upload_dirs = await _build_config(request, settings)
            if errors := config.validate():
                for directory in upload_dirs:
                    shutil.rmtree(directory, ignore_errors=True)
                return templates.TemplateResponse(
                    request=request,
                    name="new_job.html",
                    context={"errors": errors, "values": values, "accounts": store.list_accounts()},
                    status_code=422,
                )
            job_id = manager.create_job(config)
        except (ValueError, OSError) as exc:
            for directory in upload_dirs:
                shutil.rmtree(directory, ignore_errors=True)
            return templates.TemplateResponse(
                request=request,
                name="new_job.html",
                context={"errors": [str(exc)], "values": {}, "accounts": store.list_accounts()},
                status_code=422,
            )
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_detail(request: Request, job_id: int):
        job = store.get_job(job_id)
        if not job:
            return HTMLResponse("Job not found", status_code=404)
        return templates.TemplateResponse(
            request=request,
            name="job_detail.html",
            context={
                "job": _format_job(job),
                "tasks": _progress(store.get_tasks(job_id)),
                "scopes": store.get_scopes(job_id),
                "events": store.get_events(job_id, 100),
                "failed": store.ledger_failed(job_id),
                "reauth_account_id": _reauth_account_id(job, store),
            },
        )

    @app.get("/jobs/{job_id}/fragment", response_class=HTMLResponse)
    async def job_fragment(request: Request, job_id: int):
        job = store.get_job(job_id)
        if not job:
            return HTMLResponse("Job not found", status_code=404)
        return templates.TemplateResponse(
            request=request,
            name="job_content.html",
            context={
                "job": _format_job(job),
                "tasks": _progress(store.get_tasks(job_id)),
                "scopes": store.get_scopes(job_id),
                "events": store.get_events(job_id, 100),
                "failed": store.ledger_failed(job_id),
                "reauth_account_id": _reauth_account_id(job, store),
            },
        )

    @app.post("/jobs/{job_id}/{action}")
    async def job_action(job_id: int, action: str):
        try:
            if action == "pause":
                manager.pause(job_id)
            elif action in ("resume", "retry"):
                manager.resume(job_id)
            elif action == "cancel":
                manager.cancel(job_id)
            elif action == "clone":
                new_id = manager.clone(job_id)
                return RedirectResponse(f"/jobs/{new_id}", status_code=303)
            elif action == "forget":
                manager.forget_progress(job_id)
            elif action == "delete":
                manager.delete(job_id)
                return RedirectResponse("/", status_code=303)
            else:
                return HTMLResponse("Unknown action", status_code=404)
        except (KeyError, ValueError) as exc:
            return HTMLResponse(str(exc), status_code=400)
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/api/test")
    async def test_endpoint(request: Request):
        body = await request.json()
        kind = body.get("kind", "mail")
        side = body.get("side", "source")
        endpoint = Endpoint.from_dict(body.get("endpoint"))
        if not endpoint:
            return JSONResponse({"ok": False, "detail": "Endpoint is not configured.", "folders": []}, 422)
        options = Options.from_dict(body.get("options"))
        try:
            endpoint = resolve_endpoint(endpoint, load_accounts(store, secretbox), token_manager)
        except Exception as exc:
            return JSONResponse({"ok": False, "detail": str(exc), "folders": []}, 400)
        folders = []
        try:
            if endpoint.type in ("imap", "gmail") and side == "source":
                source = make_source(endpoint, options)
                try:
                    source.connect()
                    scopes = source.scopes()
                    folders = [{"source": scope.name, "target": scope.target or "labels"} for scope in scopes]
                finally:
                    source.close()
            elif endpoint.type == "jmap" and side == "dest" and kind in ("contacts", "calendars"):
                dest = JmapDavDestination(endpoint, kind)
                try:
                    dest.connect()
                    folders = [
                        {"source": item["displayname"], "target": item["id"]} for item in dest.collections
                    ]
                finally:
                    dest.close()
            elif endpoint.type in ("imap", "jmap"):
                dest = make_dest(endpoint, options)
                try:
                    dest.connect()
                    if kind == "mail" and side == "source":
                        folders = [
                            {"source": folder.name, "target": folder.role or folder.name}
                            for folder in dest.folders.values()
                        ]
                finally:
                    dest.close()
            elif endpoint.type in ("carddav", "caldav"):
                client = DavClient(endpoint, endpoint.type)
                try:
                    _home, collections = client.discover()
                    folders = [
                        {"source": item["displayname"], "target": item["href"]} for item in collections
                    ]
                finally:
                    client.close()
            elif endpoint.type == "file":
                source = DavSource(endpoint, kind)
                scopes = source.scopes()
                folders = [{"source": scope.name, "target": scope.name} for scope in scopes]
            else:
                raise ValueError(f"Unsupported endpoint type {endpoint.type!r}")
            return {"ok": True, "detail": "Connection successful.", "folders": folders}
        except Exception as exc:
            return {"ok": False, "detail": str(exc), "folders": []}

    @app.get("/api/jobs")
    async def api_jobs():
        return [manager.api_job(job["id"]) for job in store.list_jobs()]

    @app.get("/api/jobs/{job_id}")
    async def api_job(job_id: int):
        data = manager.api_job(job_id)
        return data if data else JSONResponse({"detail": "Job not found"}, 404)

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    def _account_page_data(account_id):
        account = store.get_account(account_id)
        if not account:
            return None
        secrets = secretbox.decrypt(account.get("secrets_enc"))
        return {
            "id": account["id"],
            "name": account["name"],
            "config": account["config"],
            "has_password": bool(secrets.get("password")),
            "has_token": bool(secrets.get("token")),
            "has_client_secret": bool(secrets.get("client_secret")),
            "signed_in": bool(secrets.get("access_token") and secrets.get("refresh_token")),
        }

    @app.get("/accounts", response_class=HTMLResponse)
    async def accounts_page(request: Request):
        accounts = []
        for account in store.list_accounts():
            secrets = secretbox.decrypt(account.get("secrets_enc"))
            accounts.append(
                {
                    "id": account["id"],
                    "name": account["name"],
                    "type": account["config"].get("type", ""),
                    "username": account["config"].get("username", ""),
                    "url": account["config"].get("url", ""),
                    "auth_method": account["config"].get("auth_method", "password"),
                    "signed_in": bool(secrets.get("access_token") and secrets.get("refresh_token")),
                }
            )
        return templates.TemplateResponse(
            request=request, name="accounts.html", context={"accounts": accounts}
        )

    @app.get("/accounts/new", response_class=HTMLResponse)
    async def new_account(request: Request):
        return templates.TemplateResponse(
            request=request, name="account_form.html", context={"account": None, "error": ""}
        )

    def _save_account(form, account_id=None):
        name = str(form.get("name") or "").strip()
        config = {
            "type": str(form.get("type") or "jmap"),
            "url": str(form.get("url") or "").strip(),
            "host": str(form.get("host") or "").strip(),
            "port": int(form.get("port") or 0),
            "security": str(form.get("security") or "ssl"),
            "username": str(form.get("username") or "").strip(),
            "insecure_tls": bool(form.get("insecure_tls")),
            "auth_method": str(form.get("auth_method") or "password"),
            "issuer_url": str(form.get("issuer_url") or "").strip(),
            "client_id": str(form.get("client_id") or "").strip(),
            "scopes": str(form.get("scopes") or "").strip(),
        }
        if not name:
            raise ValueError("Account name is required.")
        if config["auth_method"] not in ("password", "token", "oauth"):
            raise ValueError("Unsupported authentication method.")
        current = store.get_account(account_id) if account_id else None
        secrets = secretbox.decrypt(current.get("secrets_enc")) if current else {}
        for field in ("password", "token", "client_secret"):
            value = str(form.get(field) or "")
            if value:
                secrets[field] = value
        encrypted = secretbox.encrypt(secrets) if secrets else None
        if current:
            store.update_account_config(account_id, name, config)
            store.update_account_secrets(account_id, encrypted)
            return account_id
        return store.create_account(name, config, encrypted)

    @app.post("/accounts")
    async def create_account(request: Request):
        form = await request.form()
        try:
            account_id = _save_account(form)
        except (ValueError, OSError) as exc:
            return templates.TemplateResponse(
                request=request,
                name="account_form.html",
                context={"account": None, "error": str(exc)},
                status_code=422,
            )
        return RedirectResponse(f"/accounts/{account_id}", status_code=303)

    @app.get("/accounts/{account_id}", response_class=HTMLResponse)
    async def account_detail(request: Request, account_id: int, error: str = "", message: str = ""):
        account = _account_page_data(account_id)
        if not account:
            return HTMLResponse("Account not found", status_code=404)
        return templates.TemplateResponse(
            request=request,
            name="account_form.html",
            context={"account": account, "error": error, "message": message},
        )

    @app.post("/accounts/{account_id}")
    async def update_account(request: Request, account_id: int):
        form = await request.form()
        if not store.get_account(account_id):
            return HTMLResponse("Account not found", status_code=404)
        try:
            _save_account(form, account_id)
        except (ValueError, OSError) as exc:
            return RedirectResponse(
                f"/accounts/{account_id}?error={urlencode({'error': str(exc)})[6:]}",
                status_code=303,
            )
        return RedirectResponse(f"/accounts/{account_id}?message=Account%20saved", status_code=303)

    @app.post("/accounts/{account_id}/delete")
    async def delete_account(account_id: int):
        try:
            store.delete_account(account_id)
        except ValueError as exc:
            return RedirectResponse(
                f"/accounts/{account_id}?error={urlencode({'error': str(exc)})[6:]}",
                status_code=303,
            )
        return RedirectResponse("/accounts", status_code=303)

    @app.get("/accounts/{account_id}/signin")
    async def account_signin(account_id: int):
        try:
            return RedirectResponse(oauth.start(account_id), status_code=303)
        except Exception as exc:
            return RedirectResponse(
                f"/accounts/{account_id}?error={urlencode({'error': str(exc)})[6:]}",
                status_code=303,
            )

    @app.get("/oauth/callback", response_class=HTMLResponse)
    async def oauth_callback(request: Request):
        try:
            account = oauth.complete(str(request.url))
            return RedirectResponse(f"/accounts/{account['id']}?message=Sign-in%20complete", status_code=303)
        except Exception as exc:
            return templates.TemplateResponse(
                request=request,
                name="oauth_result.html",
                context={"error": str(exc), "account_id": None},
                status_code=400,
            )

    @app.post("/oauth/complete")
    async def oauth_complete(request: Request):
        form = await request.form()
        callback = str(form.get("callback") or "")
        try:
            account = oauth.complete(callback)
        except Exception as exc:
            return templates.TemplateResponse(
                request=request,
                name="oauth_result.html",
                context={"error": str(exc), "account_id": None},
                status_code=400,
            )
        return RedirectResponse(f"/accounts/{account['id']}?message=Sign-in%20complete", status_code=303)

    return app


_app: FastAPI | None = None


def __getattr__(name: str):
    # Lazily build the ASGI app for `uvicorn emailsync.web.app:app` so importing this module has no side effects.
    global _app
    if name == "app":
        if _app is None:
            _app = create_app()
        return _app
    raise AttributeError(name)
