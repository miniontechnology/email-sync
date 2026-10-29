import threading
import time
from datetime import datetime


class RunControl:
    def __init__(self):
        self._stop = threading.Event()
        self._abort = threading.Event()
        self._lock = threading.Lock()
        self._reason: str | None = None
        self._error: BaseException | None = None
        self._throttled_until = 0.0

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    @property
    def reason(self) -> str | None:
        with self._lock:
            return self._reason

    @property
    def aborted(self) -> bool:
        return self._abort.is_set()

    @property
    def error(self) -> BaseException | None:
        with self._lock:
            return self._error

    def stop(self, reason: str) -> None:
        if reason not in ("pause", "cancel"):
            raise ValueError("stop reason must be 'pause' or 'cancel'")
        with self._lock:
            self._reason = reason
        self._stop.set()

    def abort(self, error: BaseException) -> None:
        with self._lock:
            if self._error is None:
                self._error = error
        self._abort.set()

    def throttle(self, seconds: float) -> float:
        with self._lock:
            self._throttled_until = max(self._throttled_until, time.monotonic() + max(0, seconds))
            return self._throttled_until

    @property
    def throttled_until(self) -> float:
        with self._lock:
            return self._throttled_until

    def sleep(self, seconds: float) -> bool:
        """Wait until seconds elapse or the run stops; return True if stopped."""
        deadline = time.monotonic() + max(0, seconds)
        while not self.stopped:
            remaining = deadline - time.monotonic()
            until = self.throttled_until
            remaining = max(remaining, until - time.monotonic())
            if remaining <= 0:
                return self.stopped
            if self._stop.wait(min(remaining, 0.25)):
                return True
        return True

    def wait_throttle(self, on_wait=None) -> bool:
        until = self.throttled_until
        if until <= time.monotonic():
            return self.stopped
        if on_wait:
            when = datetime.fromtimestamp(time.time() + until - time.monotonic()).astimezone()
            on_wait(f"Throttled by server, waiting until {when:%H:%M}")
        return self.sleep(until - time.monotonic())


class TaskResult:
    def __init__(self, status: str, error: str | None = None):
        self.status = status
        self.error = error

    def __iter__(self):
        yield self.status
        yield self.error

    def __eq__(self, other):
        if isinstance(other, tuple):
            return (self.status, self.error) == other
        return isinstance(other, TaskResult) and (self.status, self.error) == (other.status, other.error)

    def __repr__(self):
        return f"TaskResult(status={self.status!r}, error={self.error!r})"
