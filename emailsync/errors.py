"""Error classification used by the retry logic."""

import imaplib
import socket
import ssl

import httpx


class SyncError(Exception):
    """Base class for errors raised by connectors."""


class TransientError(SyncError):
    """Network/server hiccup: reconnect and retry."""


class PermanentError(SyncError):
    """This item can never succeed as-is (e.g. rejected message). Record and move on."""


class AuthError(SyncError):
    """Credentials rejected: the whole task fails."""


class FatalError(SyncError):
    """Unrecoverable for the whole task (e.g. destination over quota)."""


class ThrottledError(SyncError):
    """Server asked us to slow down."""

    def __init__(self, message: str, retry_after: float = 300.0):
        super().__init__(message)
        self.retry_after = retry_after


THROTTLE_MARKERS = (
    "[throttled]",
    "bandwidth",
    "too many simultaneous",
    "too many connections",
    "rate limit",
    "try again later",
    "[unavailable]",
)
AUTH_MARKERS = (
    "[authenticationfailed]",
    "invalid credentials",
    "authentication failed",
    "[auth]",
    "login failed",
)
FATAL_MARKERS = ("[overquota]", "quota exceeded", "over quota", "mailbox full")


def classify(exc: BaseException) -> type[SyncError]:
    if isinstance(exc, SyncError):
        return type(exc)
    text = str(exc).lower()
    if any(m in text for m in THROTTLE_MARKERS):
        return ThrottledError
    if any(m in text for m in AUTH_MARKERS):
        return AuthError
    if any(m in text for m in FATAL_MARKERS):
        return FatalError
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code in (401, 403):
            return AuthError
        if code == 429:
            return ThrottledError
        if code == 507:
            return FatalError
        if code >= 500 or code in (408, 409, 423):
            return TransientError
        return PermanentError
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return TransientError
    if isinstance(exc, imaplib.IMAP4.abort):
        return TransientError
    if isinstance(
        exc, (socket.error, ssl.SSLError, TimeoutError, ConnectionError, EOFError, BrokenPipeError)
    ):
        return TransientError
    if isinstance(exc, imaplib.IMAP4.error):
        return PermanentError
    return PermanentError


def wrap(exc: BaseException) -> SyncError:
    if isinstance(exc, SyncError):
        return exc
    cls = classify(exc)
    msg = f"{type(exc).__name__}: {exc}"
    if cls is ThrottledError:
        return ThrottledError(msg)
    return cls(msg)


def backoff(attempt: int, base: float = 2.0, cap: float = 120.0) -> float:
    return min(cap, base * (2 ** max(0, attempt - 1)))
