"""Domain models and state enums.

State model
-----------
Task state:  PENDING -> RUNNING -> SUCCEEDED
                         |          |
                         |          +--> FAILED (retries exhausted)
                         +--> CANCELLED
             PENDING -> SKIPPED (upstream failed/skipped or run cancelled
             before the task ever started)

Run state:   QUEUED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED

A run is QUEUED while it waits for a global concurrency slot; no task may
start in that state and the run can be cancelled straight out of the queue.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
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
    tasks: dict[str, TaskDef]
    created_at: float = field(default_factory=time.time)

    @property
    def has_timeouts(self) -> bool:
        return any(t.timeout_seconds is not None for t in self.tasks.values())


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
    state: RunState = RunState.RUNNING
    tasks: dict[str, TaskRuntime] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    timeout_seconds: float | None = None
    reason: str | None = None
    seq: int = 0
    cancel_event: threading.Event = field(default_factory=threading.Event)
    timed_out_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.RLock = field(default_factory=threading.RLock)
    cond: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        self.cond = threading.Condition(self.lock)

    @classmethod
    def new(
        cls,
        pipeline: PipelineDef,
        idempotency_key: str | None,
        timeout_seconds: float | None = None,
        seq: int = 0,
    ) -> Run:
        return cls(
            run_id=uuid.uuid4().hex,
            pipeline_name=pipeline.name,
            version=pipeline.version,
            idempotency_key=idempotency_key,
            tasks={name: TaskRuntime(name=name) for name in pipeline.tasks},
            state=RunState.QUEUED,
            timeout_seconds=timeout_seconds,
            seq=seq,
        )
