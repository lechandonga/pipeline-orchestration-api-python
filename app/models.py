"""Domain models and state enums.

State model
-----------
Task state:  PENDING -> RUNNING -> SUCCEEDED
                         |          |
                         |          +--> FAILED (retries exhausted)
                         +--> CANCELLED
             PENDING -> SKIPPED (upstream failed/skipped or run cancelled
             before the task ever started)

Run state:   RUNNING -> SUCCEEDED | FAILED | CANCELLED
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


@dataclass
class PipelineDef:
    name: str
    version: int
    default_max_retries: int
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
    state: RunState = RunState.RUNNING
    tasks: dict[str, TaskRuntime] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
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
