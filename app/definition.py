"""流程定义的校验与不可变版本管理。"""

from __future__ import annotations

import threading

from app.errors import DefinitionError, ErrorCode
from app.models import TaskDef, WorkflowDefPayload


class WorkflowDefinition:
    """发布后的不可变快照，历史运行始终绑定该对象解释。"""

    def __init__(self, name: str, version: int, tasks: list[TaskDef]) -> None:
        self.name = name
        self.version = version
        self.tasks = [t.model_copy(deep=True) for t in tasks]
        self.tasks_by_name = {task.name: task for task in self.tasks}
        self.dependencies = {
            task.name: list(task.dependencies) for task in self.tasks
        }

    def view(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "tasks": [task.model_dump() for task in self.tasks],
        }


def validate(payload: WorkflowDefPayload) -> list[TaskDef]:
    """校验流程定义，非法时抛出带错误码的 DefinitionError。"""
    if not payload.name or not payload.name.strip():
        raise DefinitionError(
            ErrorCode.INVALID_WORKFLOW_NAME,
            "流程名不能为空",
            name=payload.name,
        )
    if not payload.tasks:
        raise DefinitionError(
            ErrorCode.EMPTY_WORKFLOW,
            f"流程 '{payload.name}' 必须至少包含一个任务",
            workflow=payload.name,
        )

    seen: set[str] = set()
    for task in payload.tasks:
        if not task.name or not task.name.strip():
            raise DefinitionError(
                ErrorCode.INVALID_TASK_NAME,
                "任务名不能为空",
                workflow=payload.name,
            )
        if task.name in seen:
            raise DefinitionError(
                ErrorCode.DUPLICATE_TASK_NAME,
                f"任务名重复: {task.name}",
                workflow=payload.name,
                task=task.name,
            )
        seen.add(task.name)

    names = {task.name for task in payload.tasks}
    for task in payload.tasks:
        for dep in task.dependencies:
            if dep == task.name:
                raise DefinitionError(
                    ErrorCode.SELF_DEPENDENCY,
                    f"任务 '{task.name}' 不能依赖自身",
                    workflow=payload.name,
                    task=task.name,
                )
            if dep not in names:
                raise DefinitionError(
                    ErrorCode.MISSING_DEPENDENCY,
                    f"任务 '{task.name}' 的依赖 '{dep}' 在流程中不存在",
                    workflow=payload.name,
                    task=task.name,
                    missing=dep,
                )

    cycle = _find_cycle({task.name: list(task.dependencies) for task in payload.tasks})
    if cycle is not None:
        raise DefinitionError(
            ErrorCode.CYCLIC_DEPENDENCY,
            f"任务依赖存在环: {' -> '.join(cycle)}",
            workflow=payload.name,
            cycle=cycle,
        )

    return [task.model_copy(deep=True) for task in payload.tasks]


def _find_cycle(graph: dict[str, list[str]]) -> list[str] | None:
    """DFS 找环；返回构成环的任务名序列，无环返回 None。"""
    white, gray, black = 0, 1, 2
    color = {node: white for node in graph}
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = gray
        stack.append(node)
        for nxt in graph[node]:
            if color[nxt] == gray:
                idx = stack.index(nxt)
                return stack[idx:] + [nxt]
            if color[nxt] == white:
                found = visit(nxt)
                if found is not None:
                    return found
        stack.pop()
        color[node] = black
        return None

    for node in graph:
        if color[node] == white:
            found = visit(node)
            if found is not None:
                return found
    return None


class WorkflowRegistry:
    """流程注册中心：每次提交生成新版本，已发布版本不可修改。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, list[WorkflowDefinition]] = {}

    def publish(self, payload: WorkflowDefPayload) -> WorkflowDefinition:
        tasks = validate(payload)
        with self._lock:
            history = self._versions.setdefault(payload.name, [])
            definition = WorkflowDefinition(payload.name, len(history) + 1, tasks)
            history.append(definition)
            return definition

    def get(self, name: str, version: int | None = None) -> WorkflowDefinition:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise DefinitionError(
                    ErrorCode.UNKNOWN_WORKFLOW,
                    f"流程 '{name}' 不存在",
                    name=name,
                )
            if version is None:
                return history[-1]
            if version < 1 or version > len(history):
                raise DefinitionError(
                    ErrorCode.UNKNOWN_VERSION,
                    f"流程 '{name}' 的版本 {version} 不存在",
                    name=name,
                    version=version,
                    latest=len(history),
                )
            return history[version - 1]

    def describe(self, name: str) -> dict:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise DefinitionError(
                    ErrorCode.UNKNOWN_WORKFLOW,
                    f"流程 '{name}' 不存在",
                    name=name,
                )
            return {
                "name": name,
                "latest_version": len(history),
                "versions": [definition.view() for definition in history],
            }
