"""Convert domain objects to JSON-serializable API views."""

from __future__ import annotations

from .models import AttemptRecord, PipelineDef, Run, TaskDef, TaskRuntime


def attempt_to_dict(attempt: AttemptRecord) -> dict:
    return {
        "attempt": attempt.attempt,
        "status": attempt.status,
        "started_at": attempt.started_at,
        "finished_at": attempt.finished_at,
        "error": attempt.error,
    }


def task_to_dict(task: TaskRuntime) -> dict:
    return {
        "name": task.name,
        "state": task.state.value,
        "attempts": [attempt_to_dict(a) for a in task.attempts],
        "attempt_count": len(task.attempts),
        "started_at": task.started_at,
        "finished_at": task.finished_at,
        "error": task.error,
    }


def run_to_dict(run: Run) -> dict:
    return {
        "run_id": run.run_id,
        "pipeline": run.pipeline_name,
        "version": run.version,
        "idempotency_key": run.idempotency_key,
        "state": run.state.value,
        "created_at": run.created_at,
        "finished_at": run.finished_at,
        "tasks": {name: task_to_dict(task) for name, task in sorted(run.tasks.items())},
    }


def task_def_to_dict(task: TaskDef) -> dict:
    return {
        "name": task.name,
        "dependencies": list(task.dependencies),
        "action": dict(task.action),
        "max_retries": task.max_retries,
    }


def definition_to_dict(definition: PipelineDef) -> dict:
    return {
        "name": definition.name,
        "version": definition.version,
        "default_max_retries": definition.default_max_retries,
        "created_at": definition.created_at,
        "tasks": [
            task_def_to_dict(definition.tasks[name])
            for name in sorted(definition.tasks)
        ],
    }
