"""Run scheduling, idempotency, cancellation and retry propagation."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor

from .errors import (
    RunNotCancellableError,
    RunNotFoundError,
)
from .executor import ExecutionContext, LocalExecutor, TaskFailure, now
from .models import (
    AttemptRecord,
    PipelineDef,
    Run,
    RunState,
    TaskDef,
    TaskState,
)
from .validation import DefinitionRegistry

log = logging.getLogger("orchestrator")

_TERMINAL_TASK_STATES = {
    TaskState.SUCCEEDED,
    TaskState.FAILED,
    TaskState.SKIPPED,
    TaskState.CANCELLED,
}


class Orchestrator:
    def __init__(
        self,
        registry: DefinitionRegistry,
        executor: LocalExecutor | None = None,
        max_workers: int = 16,
    ) -> None:
        self.registry = registry
        self.executor = executor or LocalExecutor()
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="pipe-task"
        )
        self._runs: dict[str, Run] = {}
        # (pipeline, idempotency_key) -> run_id; a run owns its key forever,
        # including after it finishes, so late re-triggers return the result.
        self._idempotency: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True, cancel_futures=False)

    # ------------------------------------------------------------------ runs
    def start_run(
        self,
        pipeline_name: str,
        *,
        version: int | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[Run, bool]:
        """Start (or return the existing run for) a pipeline run.

        Returns ``(run, created)``. The definition snapshot is captured here,
        so later submissions of new versions never affect this run.
        """
        definition = self.registry.get(pipeline_name, version)
        with self._lock:
            if idempotency_key is not None:
                existing_id = self._idempotency.get((definition.name, idempotency_key))
                if existing_id is not None:
                    existing = self._runs[existing_id]
                    log.info(
                        "idempotent replay pipeline=%s version=%d run=%s key=%s",
                        existing.pipeline_name,
                        existing.version,
                        existing.run_id,
                        idempotency_key,
                    )
                    return existing, False
            run = Run.new(definition, idempotency_key)
            self._runs[run.run_id] = run
            if idempotency_key is not None:
                self._idempotency[(definition.name, idempotency_key)] = run.run_id
        log.info(
            "run created pipeline=%s version=%d run=%s tasks=%d key=%s",
            run.pipeline_name,
            run.version,
            run.run_id,
            len(run.tasks),
            idempotency_key,
        )
        self._pool.submit(self._schedule, run, definition)
        return run, True

    def get_run(self, run_id: str) -> Run:
        with self._lock:
            run = self._runs.get(run_id)
        if run is None:
            raise RunNotFoundError(
                f"run '{run_id}' does not exist", details={"run_id": run_id}
            )
        return run

    def list_runs(self, pipeline_name: str | None = None) -> list[Run]:
        with self._lock:
            runs = list(self._runs.values())
        if pipeline_name is not None:
            runs = [r for r in runs if r.pipeline_name == pipeline_name]
        return runs

    def wait_for(self, run_id: str, timeout: float | None = None) -> Run:
        run = self.get_run(run_id)
        with run.cond:
            if run.state == RunState.RUNNING:
                run.cond.wait_for(
                    lambda: run.state != RunState.RUNNING, timeout=timeout
                )
        return run

    # ------------------------------------------------------------ cancellation
    def cancel_run(self, run_id: str) -> Run:
        run = self.get_run(run_id)
        with run.cond:
            if run.state != RunState.RUNNING:
                raise RunNotCancellableError(
                    f"run is already in terminal state {run.state.value}",
                    details={"run_id": run_id, "state": run.state.value},
                )
            run.state = RunState.CANCELLED
            run.cancel_event.set()
            for task in run.tasks.values():
                if task.state == TaskState.PENDING:
                    task.state = TaskState.SKIPPED
                    task.error = "skipped: run cancelled before start"
                    task.finished_at = now()
            run.finished_at = now()
            run.cond.notify_all()
        log.info("run cancelled run=%s", run_id)
        return run

    # -------------------------------------------------------------- scheduling
    def _schedule(self, run: Run, definition: PipelineDef) -> None:
        in_flight: dict[str, Future] = {}
        remaining = set(definition.tasks)

        try:
            while True:
                ready: list[TaskDef] = []
                with run.cond:
                    if run.state == RunState.CANCELLED:
                        break
                    failed = any(
                        t.state == TaskState.FAILED for t in run.tasks.values()
                    )
                    if failed:
                        # Downstream tasks are forbidden from starting, but
                        # unrelated in-flight branches must converge first so
                        # the run never reports FAILED while tasks are RUNNING.
                        self._propagate_skips(run, definition)
                        if all(
                            task.state in _TERMINAL_TASK_STATES
                            for task in run.tasks.values()
                        ):
                            run.state = RunState.FAILED
                            run.finished_at = now()
                            run.cond.notify_all()
                            break
                    # Launch tasks whose dependencies are settled. After a
                    # failure, propagation above has removed every blocked
                    # task from PENDING, so only healthy branches launch.
                    for name in list(remaining):
                        task_def = definition.tasks[name]
                        if all(
                            run.tasks[d].state in _TERMINAL_TASK_STATES
                            for d in task_def.dependencies
                        ):
                            remaining.discard(name)
                            ready.append(task_def)
                for task_def in ready:
                    in_flight[task_def.name] = self._pool.submit(
                        self._run_task, run, definition, task_def
                    )
                if not in_flight:
                    # Nothing running and nothing newly launched means all
                    # tasks reached a terminal state without failures.
                    break
                done = self._wait_any(run, in_flight)
                for name, future in done:
                    in_flight.pop(name)
                    future.result()  # surface unexpected worker errors
            if run.state == RunState.RUNNING:
                with run.cond:
                    run.state = RunState.SUCCEEDED
                    run.finished_at = now()
                    run.cond.notify_all()
        except Exception:  # pragma: no cover - defensive
            log.exception("scheduler crashed run=%s", run.run_id)
            with run.cond:
                if run.state == RunState.RUNNING:
                    run.state = RunState.FAILED
                    run.finished_at = now()
                    run.cond.notify_all()
        finally:
            # When cancelled, in-flight tasks may still be executing; wait for
            # them to converge so status is never self-contradictory.
            for future in list(in_flight.values()):
                future.result()

    @staticmethod
    def _wait_any(run: Run, in_flight: dict[str, Future]) -> list[str]:
        with run.cond:
            while run.state == RunState.RUNNING:
                finished = [n for n, f in in_flight.items() if f.done()]
                if finished:
                    return [(n, in_flight[n]) for n in finished]
                run.cond.wait(timeout=0.05)
                finished = [n for n, f in in_flight.items() if f.done()]
                if finished:
                    return [(n, in_flight[n]) for n in finished]
        return [(n, f) for n, f in in_flight.items() if f.done()]

    def _wait_settled(self, run: Run, timeout: float = 30.0) -> None:
        """Block until every task has converged to a terminal state."""
        with run.cond:
            run.cond.wait_for(
                lambda: all(
                    task.state in _TERMINAL_TASK_STATES for task in run.tasks.values()
                ),
                timeout=timeout,
            )

    def _propagate_skips(self, run: Run, definition: PipelineDef) -> None:
        """Mark every not-yet-started downstream of a failed task SKIPPED."""
        changed = True
        blocked_states = {TaskState.FAILED, TaskState.SKIPPED, TaskState.CANCELLED}
        while changed:
            changed = False
            for task_def in definition.tasks.values():
                task = run.tasks[task_def.name]
                if task.state != TaskState.PENDING:
                    continue
                blockers = [
                    d
                    for d in task_def.dependencies
                    if run.tasks[d].state in blocked_states
                ]
                if blockers:
                    task.state = TaskState.SKIPPED
                    task.error = "skipped: upstream did not succeed: " + ", ".join(
                        sorted(blockers)
                    )
                    task.finished_at = now()
                    log.info(
                        "task skipped run=%s task=%s blockers=%s",
                        run.run_id,
                        task_def.name,
                        blockers,
                    )
                    changed = True

    # ------------------------------------------------------------- task worker
    def _run_task(self, run: Run, definition: PipelineDef, task_def: TaskDef) -> None:
        task = run.tasks[task_def.name]
        max_attempts = task_def.max_retries + 1
        with run.cond:
            if run.state != RunState.RUNNING or task.state != TaskState.PENDING:
                return
            task.state = TaskState.RUNNING
            task.started_at = now()

        for attempt_no in range(1, max_attempts + 1):
            record = AttemptRecord(
                attempt=attempt_no, status="RUNNING", started_at=now()
            )
            task.attempts.append(record)
            ctx = ExecutionContext(
                pipeline_name=definition.name,
                task_name=task_def.name,
                attempt=attempt_no,
                cancel_event=run.cancel_event,
            )
            log.info(
                "task attempt run=%s task=%s attempt=%d/%d action=%s",
                run.run_id,
                task_def.name,
                attempt_no,
                max_attempts,
                task_def.action.get("type", "succeed"),
            )
            try:
                self.executor.execute(ctx, task_def.action)
            except TaskFailure as exc:
                record.status = "FAILED"
                record.error = str(exc)
                record.finished_at = now()
                last_error = str(exc)
                log.warning(
                    "task attempt failed run=%s task=%s attempt=%d error=%s",
                    run.run_id,
                    task_def.name,
                    attempt_no,
                    exc,
                )
                with run.cond:
                    if run.state == RunState.CANCELLED:
                        task.state = TaskState.CANCELLED
                        task.error = last_error
                        task.finished_at = now()
                        run.cond.notify_all()
                        return
                if attempt_no < max_attempts:
                    continue
                with run.cond:
                    task.state = TaskState.FAILED
                    task.error = f"failed after {attempt_no} attempt(s): {last_error}"
                    task.finished_at = now()
                    run.cond.notify_all()
                return
            except Exception:  # pragma: no cover - defensive
                log.exception(
                    "task worker error run=%s task=%s", run.run_id, task_def.name
                )
                record.status = "FAILED"
                record.error = "internal executor error"
                record.finished_at = now()
                with run.cond:
                    task.state = TaskState.FAILED
                    task.error = "internal executor error"
                    task.finished_at = now()
                    run.cond.notify_all()
                return
            else:
                record.status = "SUCCEEDED"
                record.finished_at = now()
                with run.cond:
                    if run.state == RunState.CANCELLED:
                        # Completed at the instant of cancellation: the work
                        # genuinely finished, so record it but cancel the run.
                        task.state = TaskState.CANCELLED
                        task.error = "cancelled after last attempt finished"
                        task.finished_at = now()
                        run.cond.notify_all()
                        return
                    task.state = TaskState.SUCCEEDED
                    task.finished_at = now()
                    run.cond.notify_all()
                log.info(
                    "task succeeded run=%s task=%s attempts=%d",
                    run.run_id,
                    task_def.name,
                    attempt_no,
                )
                return
