"""Encryption of stored credentials (Fernet, key from env or an auto-generated key file)."""

import base64
import hashlib
import json
import os
from pathlib import Path

from cryptography.fernet import Fernet


class SecretBox:
    def __init__(self, fernet: Fernet):
        self._fernet = fernet

    @classmethod
    def from_settings(cls, secret_key: str | None, data_dir: Path) -> "SecretBox":
        if secret_key:
            key = base64.urlsafe_b64encode(hashlib.sha256(secret_key.encode()).digest())
            return cls(Fernet(key))
        key_file = data_dir / "secret.key"
        if key_file.exists():
            return cls(Fernet(key_file.read_bytes().strip()))
        key = Fernet.generate_key()
        data_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(key)
        return cls(Fernet(key))

    def encrypt(self, data: dict) -> str:
        return self._fernet.encrypt(json.dumps(data).encode()).decode()

    def decrypt(self, token: str | None) -> dict:
        if not token:
            return {}
        return json.loads(self._fernet.decrypt(token.encode()))
