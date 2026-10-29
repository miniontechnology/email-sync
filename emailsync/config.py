import os
from dataclasses import dataclass
from pathlib import Path


def _bool(value: str | None, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    secret_key: str | None
    max_concurrent_jobs: int
    auto_resume: bool
    log_level: str

    @property
    def db_path(self) -> Path:
        return self.data_dir / "email-sync.db"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(os.environ.get("EMAIL_SYNC_DATA_DIR", "/data")),
        secret_key=os.environ.get("EMAIL_SYNC_SECRET_KEY") or None,
        max_concurrent_jobs=int(os.environ.get("EMAIL_SYNC_MAX_CONCURRENT_JOBS", "4")),
        auto_resume=_bool(os.environ.get("EMAIL_SYNC_AUTO_RESUME"), True),
        log_level=os.environ.get("EMAIL_SYNC_LOG_LEVEL", "INFO"),
    )
