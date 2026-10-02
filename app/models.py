"""Pydantic 数据模型与状态枚举。"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class RunState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class AttemptStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_TASK_STATES = {
    TaskState.SUCCESS,
    TaskState.FAILED,
    TaskState.SKIPPED,
    TaskState.CANCELLED,
}

TERMINAL_RUN_STATES = {
    RunState.SUCCEEDED,
    RunState.FAILED,
    RunState.CANCELLED,
}


class TaskDef(BaseModel):
    name: str = Field(..., description="任务名，同一流程内唯一")
    dependencies: list[str] = Field(default_factory=list)
    retries: int = Field(0, ge=0, description="失败后的额外重试次数，0 表示只尝试一次")
    retry_delay_seconds: float = Field(0.0, ge=0.0)


class WorkflowDefPayload(BaseModel):
    name: str
    tasks: list[TaskDef]


class Attempt(BaseModel):
    attempt: int
    status: AttemptStatus
    error: str | None = None
    started_at: datetime
    finished_at: datetime


class TaskRuntime(BaseModel):
    name: str
    state: TaskState
    attempts: list[Attempt] = Field(default_factory=list)
    reason: str | None = None


class RunView(BaseModel):
    run_id: str
    workflow_name: str
    definition_version: int
    state: RunState
    idempotency_key: str | None = None
    deduplicated: bool = False
    created_at: datetime
    finished_at: datetime | None = None
    tasks: list[TaskRuntime]


class TriggerRunPayload(BaseModel):
    idempotency_key: str | None = None
    version: int | None = Field(None, description="不传则使用当前最新版本；运行与版本快照绑定")
    # 仅用于本地模拟：{任务名: {"fail_times": 前N次尝试失败, "duration_seconds": 单次尝试耗时}}
    simulations: dict[str, dict[str, float | int]] | None = None
