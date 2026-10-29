import threading
import time

from emailsync.errors import ThrottledError, TransientError
from emailsync.jobs.control import RunControl
from emailsync.jobs.retry import retry_call


class Reconnectable:
    def __init__(self):
        self.reconnects = 0

    def reconnect(self):
        self.reconnects += 1


def test_retry_reconnects_on_transient_error(monkeypatch):
    control = RunControl()
    connection = Reconnectable()
    monkeypatch.setattr("emailsync.jobs.retry.backoff", lambda _attempt: 0)
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientError("temporary")
        return "ok"

    assert retry_call(operation, control, connection=connection) == "ok"
    assert calls == 2
    assert connection.reconnects == 1


def test_throttle_retries_without_reconnect_or_attempt_failure():
    control = RunControl()
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ThrottledError("slow down", 0)
        return "ok"

    assert retry_call(operation, control) == "ok"
    assert calls == 2


def test_control_sleep_wakes_on_stop():
    control = RunControl()
    thread = threading.Thread(target=lambda: (time.sleep(0.02), control.stop("cancel")))
    thread.start()
    started = time.monotonic()
    assert control.sleep(10)
    thread.join()
    assert time.monotonic() - started < 1
