"""Local simulated task executor.

No external systems are contacted. Action types:

* ``succeed``            - completes immediately
* ``fail``               - fails with ``message`` on every attempt
* ``flaky``              - fails until attempt number >= ``succeed_on_attempt``
* ``sleep``              - sleeps ``seconds`` then succeeds; interruption is
                           cooperative via the cancel event so cancelling a
                           run converges in-flight tasks promptly

Tests may also register forced failures through ``force_fail``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


class TaskFailure(Exception):
    pass


@dataclass
class ExecutionContext:
    pipeline_name: str
    task_name: str
    attempt: int
    cancel_event: threading.Event
    deadline: float | None = None


class LocalExecutor:
    def __init__(self) -> None:
        self._force_fail: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()

    def force_fail(self, pipeline: str, task: str, message: str = "forced") -> None:
        with self._lock:
            self._force_fail[(pipeline, task)] = message

    def clear_forced_failures(self) -> None:
        with self._lock:
            self._force_fail.clear()

    def execute(self, ctx: ExecutionContext, action: dict) -> None:
        action_type = action.get("type", "succeed")
        with self._lock:
            forced = self._force_fail.get((ctx.pipeline_name, ctx.task_name))
        if forced is not None:
            raise TaskFailure(forced)

        if action_type == "succeed":
            return
        if action_type == "fail":
            raise TaskFailure(action.get("message", "simulated failure"))
        if action_type == "flaky":
            threshold = action.get("succeed_on_attempt", 1)
            seconds = action.get("seconds", 0.0)
            self._interruptible_sleep(
                seconds, ctx.cancel_event, deadline=ctx.deadline
            )
            if ctx.attempt < threshold:
                raise TaskFailure(
                    action.get(
                        "message",
                        f"flaky failure on attempt {ctx.attempt}",
                    )
                )
            return
        if action_type == "sleep":
            self._interruptible_sleep(
                action.get("seconds", 0.05),
                ctx.cancel_event,
                deadline=ctx.deadline,
            )
            return
        raise TaskFailure(f"unknown action type '{action_type}'")

    @staticmethod
    def _interruptible_sleep(
        seconds: float,
        cancel_event: threading.Event,
        *,
        deadline: float | None = None,
    ) -> None:
        remaining = max(0.0, float(seconds))
        if deadline is not None:
            remaining = min(remaining, max(0.0, deadline - time.monotonic()))
        if cancel_event.wait(timeout=remaining):
            raise TaskFailure("interrupted by cancellation")
        if deadline is not None and time.monotonic() >= deadline:
            raise TaskTimeoutError("task attempt timed out")


class TaskTimeoutError(TaskFailure):
    """Raised when a task attempt exceeds its configured timeout."""

    def __init__(self, message: str = "task attempt timed out") -> None:
        super().__init__(message)


def now() -> float:
    return time.monotonic()
