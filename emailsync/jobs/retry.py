from collections.abc import Callable

from emailsync.errors import (
    AuthError,
    FatalError,
    PermanentError,
    ThrottledError,
    TransientError,
    backoff,
    wrap,
)

MAX_ATTEMPTS = 5
MAX_THROTTLES = 50


def reconnect(connection) -> None:
    method = getattr(connection, "reconnect", None)
    if method:
        method()
        return
    close = getattr(connection, "close", None)
    if close:
        close()
    connect = getattr(connection, "connect", None)
    if connect:
        connect()


def retry_call(
    operation: Callable,
    control,
    *,
    connection=None,
    on_wait=None,
    attempts: int = MAX_ATTEMPTS,
    on_retry: Callable[[int, BaseException], None] | None = None,
):
    attempt = 0
    throttles = 0
    while not control.stopped and not control.aborted:
        if control.wait_throttle(on_wait):
            break
        try:
            return operation()
        except BaseException as exc:
            error = wrap(exc)
            if isinstance(error, (AuthError, FatalError)):
                raise error from exc
            if isinstance(error, ThrottledError):
                throttles += 1
                if throttles > MAX_THROTTLES:
                    raise error from exc
                control.throttle(error.retry_after)
                continue
            if isinstance(error, TransientError):
                attempt += 1
                if attempt >= attempts:
                    raise error from exc
                if on_retry:
                    on_retry(attempt, error)
                try:
                    if connection is not None:
                        reconnect(connection)
                except BaseException as reconnect_exc:
                    reconnect_error = wrap(reconnect_exc)
                    if isinstance(reconnect_error, (AuthError, FatalError)):
                        raise reconnect_error from reconnect_exc
                if control.sleep(backoff(attempt)):
                    break
                continue
            if isinstance(error, PermanentError):
                raise error from exc
            raise error from exc
    return None
