"""FastAPI application and browser UI."""

import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import UploadFile as StarletteUploadFile

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
    if endpoint_type == "jmap" and mail_endpoint and mail_endpoint.type == "jmap":
        url = url or mail_endpoint.url
        if not password:
            token = token or mail_endpoint.token
    return Endpoint(
        type=endpoint_type,
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
    manager = manager or JobManager(
        store, SecretBox.from_settings(settings.secret_key, settings.data_dir), settings
    )

    @asynccontextmanager
    async def lifespan(_app):
        manager.start()
        yield
        manager.stop()

    app = FastAPI(title="Email Sync", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.manager = manager
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
            request=request, name="new_job.html", context={"errors": [], "values": {}}
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
                    context={"errors": errors, "values": values},
                    status_code=422,
                )
            job_id = manager.create_job(config)
        except (ValueError, OSError) as exc:
            for directory in upload_dirs:
                shutil.rmtree(directory, ignore_errors=True)
            return templates.TemplateResponse(
                request=request,
                name="new_job.html",
                context={"errors": [str(exc)], "values": {}},
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
