"""Pipeline definition submission and immutable versioning.

Validation order (each failure gets its own distinguishable error code):
1. payload shape and action parameters -> INVALID_ACTION / EMPTY_DEFINITION
2. duplicate task names                 -> DUPLICATE_TASK
3. self dependencies                   -> SELF_DEPENDENCY
4. dependencies pointing nowhere       -> MISSING_DEPENDENCY
5. dependency cycles (Kahn's algorithm)-> CYCLIC_DEPENDENCY

Published definitions are immutable: every run keeps the exact
``PipelineDef`` snapshot it was started against.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .errors import (
    CyclicDependencyError,
    DuplicateTaskError,
    EmptyDefinitionError,
    InvalidActionError,
    MissingDependencyError,
    PipelineNotFoundError,
    SelfDependencyError,
)
from .models import PipelineDef, TaskDef

_SUPPORTED_ACTIONS = {"succeed", "fail", "flaky", "sleep"}


def _validate_action(name: str, raw: object) -> dict:
    if raw is None:
        return {"type": "succeed"}
    if not isinstance(raw, dict):
        raise InvalidActionError(
            f"task '{name}' action must be an object",
            details={"task": name},
        )
    action_type = raw.get("type", "succeed")
    if action_type not in _SUPPORTED_ACTIONS:
        raise InvalidActionError(
            f"task '{name}' uses unsupported action type '{action_type}'",
            details={
                "task": name,
                "type": action_type,
                "supported": sorted(_SUPPORTED_ACTIONS),
            },
        )
    params = dict(raw)
    params["type"] = action_type
    if action_type in ("flaky", "sleep"):
        seconds = params.get("seconds", 0.05)
        if not isinstance(seconds, (int, float)) or seconds < 0:
            raise InvalidActionError(
                f"task '{name}' action '{action_type}' needs non-negative"
                " numeric 'seconds'",
                details={"task": name},
            )
    if action_type == "flaky":
        succeed_on_attempt = params.get("succeed_on_attempt")
        if succeed_on_attempt is not None and (
            not isinstance(succeed_on_attempt, int)
            or isinstance(succeed_on_attempt, bool)
            or succeed_on_attempt < 1
        ):
            raise InvalidActionError(
                f"task '{name}' flaky action needs positive-int 'succeed_on_attempt'",
                details={"task": name},
            )
    if "message" in params and not isinstance(params["message"], str):
        raise InvalidActionError(
            f"task '{name}' action 'message' must be a string",
            details={"task": name},
        )
    return params


def _validate_retry(value: object, where: str) -> int:
    if value is None:
        return 0
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise InvalidActionError(
            f"'{where}' must be a non-negative integer", details={"where": where}
        )
    return value


def _validate_timeout(value: object, where: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise InvalidActionError(
            f"'{where}' must be a positive number of seconds",
            details={"where": where},
        )
    return float(value)


def build_definition(payload: dict, version: int) -> PipelineDef:
    if not isinstance(payload, dict):
        raise EmptyDefinitionError("pipeline definition must be an object")
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise EmptyDefinitionError("pipeline definition requires a 'name'")
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise EmptyDefinitionError(
            "pipeline definition requires a non-empty 'tasks' list",
            details={"pipeline": name},
        )

    default_retries = _validate_retry(
        payload.get("default_max_retries", 0), "default_max_retries"
    )

    seen: set[str] = set()
    duplicates: list[str] = []
    tasks: dict[str, TaskDef] = {}
    for index, raw in enumerate(raw_tasks):
        if (
            not isinstance(raw, dict)
            or not isinstance(raw.get("name"), str)
            or not raw["name"].strip()
        ):
            raise EmptyDefinitionError(
                f"task at index {index} requires a string 'name'",
                details={"index": index},
            )
        task_name = raw["name"]
        deps = raw.get("dependencies", [])
        if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
            raise InvalidActionError(
                f"task '{task_name}' dependencies must be a list of strings",
                details={"task": task_name},
            )
        action = _validate_action(task_name, raw.get("action"))
        task_timeout = _validate_timeout(
            raw.get("timeout_seconds"), f"tasks[{index}].timeout_seconds"
        )
        raw_task_retries = raw.get("max_retries")
        task_retries = (
            None
            if raw_task_retries is None
            else _validate_retry(raw_task_retries, f"tasks[{index}].max_retries")
        )
        if task_name in seen:
            duplicates.append(task_name)
        seen.add(task_name)
        tasks[task_name] = TaskDef(
            name=task_name,
            dependencies=list(deps),
            action=action,
            max_retries=(default_retries if task_retries is None else task_retries),
            timeout_seconds=task_timeout,
        )
    if duplicates:
        raise DuplicateTaskError(
            "duplicate task names in definition",
            details={"duplicates": sorted(set(duplicates))},
        )

    for task in tasks.values():
        if task.name in task.dependencies:
            raise SelfDependencyError(
                f"task '{task.name}' depends on itself",
                details={"task": task.name},
            )
    missing = {
        task.name: [d for d in task.dependencies if d not in tasks]
        for task in tasks.values()
    }
    missing = {t: deps for t, deps in missing.items() if deps}
    if missing:
        raise MissingDependencyError(
            "tasks reference dependencies that are not defined",
            details={"missing": missing},
        )

    _assert_acyclic(tasks)

    return PipelineDef(
        name=name.strip(),
        version=version,
        default_max_retries=default_retries,
        tasks=tasks,
    )


def _assert_acyclic(tasks: dict[str, TaskDef]) -> None:
    indegree = {name: 0 for name in tasks}
    dependents: dict[str, list[str]] = {name: [] for name in tasks}
    for task in tasks.values():
        for dep in task.dependencies:
            indegree[task.name] += 1
            dependents[dep].append(task.name)
    queue = [name for name, deg in indegree.items() if deg == 0]
    visited: list[str] = []
    while queue:
        node = queue.pop()
        visited.append(node)
        for child in dependents[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(visited) != len(tasks):
        cyclic = sorted(name for name, deg in indegree.items() if deg > 0)
        raise CyclicDependencyError(
            "dependency graph contains a cycle",
            details={"tasks_in_cycle": cyclic},
        )


@dataclass
class SubmissionResult:
    name: str
    version: int
    reused: bool
    definition: PipelineDef


class DefinitionRegistry:
    """Stores immutable versioned definitions per pipeline name."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, list[PipelineDef]] = {}

    def submit(self, payload: dict) -> SubmissionResult:
        name = payload.get("name")
        with self._lock:
            lookup = name.strip() if isinstance(name, str) else name
            existing = self._versions.get(lookup, [])
            candidate = build_definition(payload, version=len(existing) + 1)
            match = next(
                (v for v in existing if self._same_graph(v, candidate)), None
            )
            if match is not None:
                return SubmissionResult(
                    name=candidate.name,
                    version=match.version,
                    reused=True,
                    definition=match,
                )
            self._versions.setdefault(candidate.name, []).append(candidate)
            return SubmissionResult(
                name=candidate.name,
                version=candidate.version,
                reused=False,
                definition=candidate,
            )

    @staticmethod
    def _same_graph(a: PipelineDef, b: PipelineDef) -> bool:
        if a.default_max_retries != b.default_max_retries:
            return False
        if set(a.tasks) != set(b.tasks):
            return False
        for task_name, task_a in a.tasks.items():
            task_b = b.tasks[task_name]
            if sorted(task_a.dependencies) != sorted(task_b.dependencies):
                return False
            if task_a.max_retries != task_b.max_retries:
                return False
            if task_a.timeout_seconds != task_b.timeout_seconds:
                return False
            if task_a.action != task_b.action:
                return False
        return True

    def get(self, name: str, version: int | None = None) -> PipelineDef:
        with self._lock:
            versions = self._versions.get(name.strip() if isinstance(name, str) else name)
            if not versions:
                raise PipelineNotFoundError(
                    f"pipeline '{name}' is not defined",
                    details={"pipeline": name},
                )
            if version is None:
                return versions[-1]
            if version < 1 or version > len(versions):
                raise PipelineNotFoundError(
                    f"pipeline '{name}' version {version} does not exist",
                    details={
                        "pipeline": name,
                        "version": version,
                        "latest": len(versions),
                    },
                )
            return versions[version - 1]

    def list_versions(self, name: str) -> list[PipelineDef]:
        with self._lock:
            versions = self._versions.get(
                name.strip() if isinstance(name, str) else name
            )
            if not versions:
                raise PipelineNotFoundError(
                    f"pipeline '{name}' is not defined",
                    details={"pipeline": name},
                )
            return list(versions)
