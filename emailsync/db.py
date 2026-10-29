"""SQLite persistence: jobs, per-task progress, the migration ledger and event log.

Each thread gets its own connection; writes are serialized through a process-wide lock so that
many worker threads never fight over SQLite's single writer.
"""

import json
import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json TEXT NOT NULL,
    secrets_enc TEXT,
    message TEXT,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    next_run_at REAL,
    run_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tasks (
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    already INTEGER NOT NULL DEFAULT 0,
    copied INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    bytes INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    started_at REAL,
    finished_at REAL,
    PRIMARY KEY (job_id, kind)
);
CREATE TABLE IF NOT EXISTS scopes (
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    already INTEGER NOT NULL DEFAULT 0,
    copied INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (job_id, kind, name)
);
CREATE TABLE IF NOT EXISTS ledger (
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    source_key TEXT NOT NULL,
    match_key TEXT,
    targets TEXT,
    status TEXT NOT NULL,
    dest_ref TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    info TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (job_id, kind, source_key)
);
CREATE INDEX IF NOT EXISTS ledger_status ON ledger(job_id, kind, status);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_job ON events(job_id, id);
"""

# Ledger statuses
UPLOADING = "uploading"  # write to destination started but not confirmed
DONE = "done"  # copied by us
SKIPPED = "skipped"  # already present in the destination
FAILED = "failed"
SETTLED = (DONE, SKIPPED)

MAX_EVENTS_PER_JOB = 2000


class Store:
    def __init__(self, path: Path | str):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._write_lock = threading.RLock()
        with self._write_lock:
            # executescript manages its own transaction, so it must not run inside write().
            self.conn().executescript(SCHEMA)

    # -- connections -------------------------------------------------------------------------
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=60, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=60000")
            self._local.conn = conn
        return conn

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            conn = self.conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return self.conn().execute(sql, tuple(params)).fetchall()

    # -- jobs --------------------------------------------------------------------------------
    def create_job(self, name: str, config: dict, secrets_enc: str | None, status: str = "queued") -> int:
        now = time.time()
        with self.write() as conn:
            cur = conn.execute(
                "INSERT INTO jobs(name, status, config_json, secrets_enc, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?)",
                (name, status, json.dumps(config), secrets_enc, now, now),
            )
            return int(cur.lastrowid)

    def get_job(self, job_id: int) -> dict | None:
        rows = self.query("SELECT * FROM jobs WHERE id=?", (job_id,))
        return _job_row(rows[0]) if rows else None

    def list_jobs(self) -> list[dict]:
        return [_job_row(r) for r in self.query("SELECT * FROM jobs ORDER BY id DESC")]

    def update_job(self, job_id: int, **fields: Any) -> None:
        if "config" in fields:
            fields["config_json"] = json.dumps(fields.pop("config"))
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.write() as conn:
            conn.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))

    def delete_job(self, job_id: int) -> None:
        with self.write() as conn:
            for table in ("ledger", "events", "scopes", "tasks"):
                conn.execute(f"DELETE FROM {table} WHERE job_id=?", (job_id,))
            conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))

    # -- tasks & scopes (progress) -----------------------------------------------------------
    def reset_task(self, job_id: int, kind: str, status: str) -> None:
        now = time.time()
        with self.write() as conn:
            conn.execute(
                "INSERT INTO tasks(job_id, kind, status, started_at) VALUES (?,?,?,?)"
                " ON CONFLICT(job_id, kind) DO UPDATE SET status=excluded.status, total=0, already=0,"
                " copied=0, skipped=0, failed=0, bytes=0, message=NULL, started_at=excluded.started_at,"
                " finished_at=NULL",
                (job_id, kind, status, now),
            )
            conn.execute("DELETE FROM scopes WHERE job_id=? AND kind=?", (job_id, kind))

    def update_task(self, job_id: int, kind: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.write() as conn:
            conn.execute(
                f"UPDATE tasks SET {cols} WHERE job_id=? AND kind=?", (*fields.values(), job_id, kind)
            )

    def get_tasks(self, job_id: int) -> list[dict]:
        return [dict(r) for r in self.query("SELECT * FROM tasks WHERE job_id=? ORDER BY kind", (job_id,))]

    def save_scopes(self, job_id: int, kind: str, scopes: dict[str, dict]) -> None:
        with self.write() as conn:
            for name, s in scopes.items():
                conn.execute(
                    "INSERT INTO scopes(job_id, kind, name, total, already, copied, skipped, failed, status)"
                    " VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id, kind, name) DO UPDATE SET"
                    " total=excluded.total, already=excluded.already, copied=excluded.copied,"
                    " skipped=excluded.skipped, failed=excluded.failed, status=excluded.status",
                    (
                        job_id,
                        kind,
                        name,
                        s.get("total", 0),
                        s.get("already", 0),
                        s.get("copied", 0),
                        s.get("skipped", 0),
                        s.get("failed", 0),
                        s.get("status", "pending"),
                    ),
                )

    def get_scopes(self, job_id: int) -> list[dict]:
        return [
            dict(r) for r in self.query("SELECT * FROM scopes WHERE job_id=? ORDER BY kind, name", (job_id,))
        ]

    # -- ledger ------------------------------------------------------------------------------
    def ledger_get_many(self, job_id: int, kind: str, keys: Iterable[str]) -> dict[str, dict]:
        keys = list(keys)
        out: dict[str, dict] = {}
        for i in range(0, len(keys), 500):
            chunk = keys[i : i + 500]
            marks = ",".join("?" * len(chunk))
            rows = self.query(
                f"SELECT * FROM ledger WHERE job_id=? AND kind=? AND source_key IN ({marks})",
                (job_id, kind, *chunk),
            )
            for r in rows:
                d = dict(r)
                d["targets"] = json.loads(d["targets"]) if d["targets"] else []
                out[d["source_key"]] = d
        return out

    def ledger_mark(
        self,
        job_id: int,
        kind: str,
        source_key: str,
        status: str,
        *,
        match_key: str | None = None,
        targets: list[str] | None = None,
        dest_ref: str | None = None,
        error: str | None = None,
        info: str | None = None,
        count_attempt: bool = False,
    ) -> None:
        with self.write() as conn:
            conn.execute(
                "INSERT INTO ledger(job_id, kind, source_key, match_key, targets, status, dest_ref, attempts,"
                " error, info, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(job_id, kind, source_key) DO UPDATE SET"
                " match_key=COALESCE(excluded.match_key, match_key),"
                " targets=COALESCE(excluded.targets, targets), status=excluded.status,"
                " dest_ref=COALESCE(excluded.dest_ref, dest_ref), attempts=attempts+excluded.attempts,"
                " error=excluded.error, info=COALESCE(excluded.info, info), updated_at=excluded.updated_at",
                (
                    job_id,
                    kind,
                    source_key,
                    match_key,
                    json.dumps(targets) if targets is not None else None,
                    status,
                    dest_ref,
                    1 if count_attempt else 0,
                    error,
                    info,
                    time.time(),
                ),
            )

    def ledger_counts(self, job_id: int, kind: str | None = None) -> dict[str, int]:
        if kind:
            rows = self.query(
                "SELECT status, COUNT(*) c FROM ledger WHERE job_id=? AND kind=? GROUP BY status",
                (job_id, kind),
            )
        else:
            rows = self.query(
                "SELECT status, COUNT(*) c FROM ledger WHERE job_id=? GROUP BY status", (job_id,)
            )
        return {r["status"]: r["c"] for r in rows}

    def ledger_items(
        self, job_id: int, kind: str, status: str | None = None, limit: int | None = 1000
    ) -> list[dict]:
        sql = "SELECT * FROM ledger WHERE job_id=? AND kind=?"
        params: list[Any] = [job_id, kind]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        if limit is None:
            sql = sql.removesuffix(" LIMIT ?")
        else:
            params.append(limit)
        out = []
        for r in self.query(sql, params):
            d = dict(r)
            d["targets"] = json.loads(d["targets"]) if d["targets"] else []
            out.append(d)
        return out

    def ledger_failed(self, job_id: int, limit: int = 200) -> list[dict]:
        rows = self.query(
            "SELECT * FROM ledger WHERE job_id=? AND status=? ORDER BY updated_at DESC LIMIT ?",
            (job_id, FAILED, limit),
        )
        return [dict(r) for r in rows]

    def ledger_reset(self, job_id: int) -> None:
        with self.write() as conn:
            conn.execute("DELETE FROM ledger WHERE job_id=?", (job_id,))

    # -- events ------------------------------------------------------------------------------
    def add_event(self, job_id: int, level: str, message: str) -> None:
        with self.write() as conn:
            cur = conn.execute(
                "INSERT INTO events(job_id, ts, level, message) VALUES (?,?,?,?)",
                (job_id, time.time(), level, message[:4000]),
            )
            if cur.lastrowid % 200 == 0:
                conn.execute(
                    "DELETE FROM events WHERE job_id=? AND id <= ?",
                    (job_id, cur.lastrowid - MAX_EVENTS_PER_JOB),
                )

    def get_events(self, job_id: int, limit: int = 200) -> list[dict]:
        rows = self.query("SELECT * FROM events WHERE job_id=? ORDER BY id DESC LIMIT ?", (job_id, limit))
        return [dict(r) for r in rows]


def _job_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["config"] = json.loads(d.pop("config_json"))
    return d
