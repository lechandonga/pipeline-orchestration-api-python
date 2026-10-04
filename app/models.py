"""Domain models and state enums.

State model
-----------
Task state:  PENDING -> RUNNING -> SUCCEEDED
                         |          |
                         |          +--> FAILED (retries/timeout exhausted)
                         +--> CANCELLED
             PENDING -> SKIPPED (upstream failed/skipped/cancelled, run
             cancelled/timed out before the task ever started)

Run state:   QUEUED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class RunState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class TaskDef:
    name: str
    dependencies: list[str]
    action: dict
    max_retries: int
    timeout_seconds: float | None = None


@dataclass
class PipelineDef:
    name: str
    version: int
    default_max_retries: int
    default_task_timeout_seconds: float | None
    tasks: dict[str, TaskDef]
    created_at: float = field(default_factory=time.time)


@dataclass
class AttemptRecord:
    attempt: int
    status: str  # SUCCEEDED | FAILED
    started_at: float
    finished_at: float | None = None
    error: str | None = None


@dataclass
class TaskRuntime:
    name: str
    state: TaskState = TaskState.PENDING
    attempts: list[AttemptRecord] = field(default_factory=list)
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None


@dataclass
class Run:
    run_id: str
    pipeline_name: str
    version: int
    idempotency_key: str | None
    state: RunState = RunState.QUEUED
    tasks: dict[str, TaskRuntime] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    wall_created_at: float = field(default_factory=time.time)
    wall_started_at: float | None = None
    wall_finished_at: float | None = None
    timeout_seconds: float | None = None
    failure_reason: str | None = None
    # While an abort (cancel or run-level timeout) is converging in-flight
    # tasks the run stays RUNNING externally; these fields describe the
    # terminal state that will be published once every task has settled, so
    # no reader can ever observe a terminal run alongside RUNNING tasks.
    abort_target: RunState | None = None
    abort_reason: str | None = None
    sequence: int = 0
    cancel_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.RLock = field(default_factory=threading.RLock)
    cond: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        self.cond = threading.Condition(self.lock)

    @classmethod
    def new(cls, pipeline: PipelineDef, idempotency_key: str | None) -> Run:
        return cls(
            run_id=uuid.uuid4().hex,
            pipeline_name=pipeline.name,
            version=pipeline.version,
            idempotency_key=idempotency_key,
            tasks={name: TaskRuntime(name=name) for name in pipeline.tasks},
        )

    def snapshot(self) -> Run:
        """Return a detached deep-enough copy for reading outside the lock."""
        with self.cond:
            copies = {
                name: TaskRuntime(
                    name=task.name,
                    state=task.state,
                    attempts=[replace(a) for a in task.attempts],
                    started_at=task.started_at,
                    finished_at=task.finished_at,
                    error=task.error,
                )
                for name, task in self.tasks.items()
            }
            return replace(self, tasks=copies)
