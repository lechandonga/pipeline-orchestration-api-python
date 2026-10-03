"""Run admission/queueing, scheduling, idempotency, cancellation, timeouts.

Concurrency model
-----------------
Two thread pools are kept separate so they can never deadlock each other:

* ``_driver_pool`` - one driver thread per created run; it waits in FIFO
                     order for a global concurrency slot, then runs the
                     dependency-scheduling loop.
* ``_task_pool``   - executes task attempts.

The global cap ``max_concurrent_runs`` limits how many runs are RUNNING at
once; every other run stays visibly QUEUED, admitted strictly by trigger
order (a monotonic sequence number).
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from .errors import (
    RunNotCancellableError,
    RunNotFoundError,
)
from .executor import ExecutionContext, LocalExecutor, TaskFailure
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

_TERMINAL_RUN_STATES = {
    RunState.SUCCEEDED,
    RunState.FAILED,
    RunState.CANCELLED,
}


@dataclass
class RunQuery:
    pipeline_name: str | None = None
    state: str | None = None
    created_from: float | None = None
    created_to: float | None = None
    limit: int = 50
    offset: int = 0


@dataclass
class RunPage:
    runs: list[Run]
    total: int
    limit: int
    offset: int


def _wall_now() -> float:
    return time.time()


class _AdmissionGate:
    """Global FIFO concurrency cap: slots are granted strictly by order."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._cond = threading.Condition()
        self._active = 0
        self._ticket = 0
        self._next_ticket = 0

    def acquire(self) -> None:
        with self._cond:
            ticket = self._ticket
            self._ticket += 1
            while self._active >= self.capacity or ticket != self._next_ticket:
                self._cond.wait()
            self._active += 1
            self._next_ticket += 1

    def release(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def active_count(self) -> int:
        with self._cond:
            return self._active


class Orchestrator:
    def __init__(
        self,
        registry: DefinitionRegistry,
        executor: LocalExecutor | None = None,
        max_workers: int = 64,
        max_concurrent_runs: int = 8,
    ) -> None:
        if max_concurrent_runs < 1:
            raise ValueError("max_concurrent_runs must be >= 1")
        self.registry = registry
        self.executor = executor or LocalExecutor()
        self._task_pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="pipe-task"
        )
        self._driver_pool = ThreadPoolExecutor(
            max_workers=512, thread_name_prefix="pipe-driver"
        )
        self.max_concurrent_runs = max_concurrent_runs
        self._gate = _AdmissionGate(max_concurrent_runs)
        self._runs: dict[str, Run] = {}
        # (pipeline, idempotency_key) -> run_id; a run owns its key forever,
        # including after it finishes, so late re-triggers return the result.
        self._idempotency: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()
        self._seq = 0

    def shutdown(self) -> None:
        self._driver_pool.shutdown(wait=True, cancel_futures=False)
        self._task_pool.shutdown(wait=True, cancel_futures=False)

    # ------------------------------------------------------------------ runs
    def start_run(
        self,
        pipeline_name: str,
        *,
        version: int | None = None,
        idempotency_key: str | None = None,
        timeout_seconds: float | None = None,
    ) -> tuple[Run, bool]:
        """Start (or replay) a pipeline run; returns ``(run, created)``.

        New runs start QUEUED; a driver waits on the admission gate. The
        definition snapshot is captured here, so later submissions never
        affect an existing run.
        """
        definition = self.registry.get(pipeline_name, version)
        with self._lock:
            if idempotency_key is not None:
                existing_id = self._idempotency.get(
                    (definition.name, idempotency_key)
                )
                if existing_id is not None:
                    existing = self._runs[existing_id]
                    log.info(
                        "idempotent replay pipeline=%s run=%s key=%s",
                        existing.pipeline_name,
                        existing.run_id,
                        idempotency_key,
                    )
                    return existing, False
            self._seq += 1
            run = Run.new(
                definition,
                idempotency_key,
                timeout_seconds=timeout_seconds,
                seq=self._seq,
            )
            self._runs[run.run_id] = run
            if idempotency_key is not None:
                self._idempotency[(definition.name, idempotency_key)] = run.run_id
        log.info(
            "run queued pipeline=%s version=%d run=%s seq=%d tasks=%d",
            run.pipeline_name,
            run.version,
            run.run_id,
            run.seq,
            len(run.tasks),
        )
        self._driver_pool.submit(self._drive, run, definition)
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
        return sorted(runs, key=lambda r: r.seq)

    def _snapshot(self) -> list[Run]:
        with self._lock:
            return sorted(self._runs.values(), key=lambda r: r.seq)

    def query_runs(self, query: RunQuery) -> RunPage:
        """Filter + paginate over a snapshot taken under a short lock.

        Runs only transition state and the registry only grows, so the
        snapshot stays internally consistent; filtering happens outside the
        lock and never blocks scheduling.
        """
        matched = [
            r
            for r in self._snapshot()
            if (query.pipeline_name is None or r.pipeline_name == query.pipeline_name)
            and (query.state is None or r.state.value == query.state)
            and (query.created_from is None or r.created_at >= query.created_from)
            and (query.created_to is None or r.created_at <= query.created_to)
        ]
        total = len(matched)
        page = matched[query.offset : query.offset + query.limit]
        return RunPage(runs=page, total=total, limit=query.limit, offset=query.offset)

    def run_stats(
        self,
        pipeline_name: str | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
    ) -> dict:
        selected = [
            r
            for r in self._snapshot()
            if (pipeline_name is None or r.pipeline_name == pipeline_name)
            and (created_from is None or r.created_at >= created_from)
            and (created_to is None or r.created_at <= created_to)
        ]
        counts = {state.value: 0 for state in RunState}
        durations: list[float] = []
        for run in selected:
            counts[run.state.value] += 1
            if run.finished_at is not None:
                durations.append(run.finished_at - (run.started_at or run.created_at))
        return {
            "total": len(selected),
            "by_state": counts,
            "queued": counts[RunState.QUEUED.value],
            "running_slots_active": self._gate.active_count(),
            "running_slots_capacity": self.max_concurrent_runs,
            "completed": len(durations),
            "average_duration_seconds": (
                sum(durations) / len(durations) if durations else None
            ),
        }

    def wait_for(self, run_id: str, timeout: float | None = None) -> Run:
        run = self.get_run(run_id)
        with run.cond:
            if run.state not in _TERMINAL_RUN_STATES:
                run.cond.wait_for(
                    lambda: run.state in _TERMINAL_RUN_STATES, timeout=timeout
                )
        return run

    # ------------------------------------------------------------ cancellation
    def cancel_run(self, run_id: str) -> Run:
        run = self.get_run(run_id)
        with run.cond:
            if run.state in _TERMINAL_RUN_STATES:
                raise RunNotCancellableError(
                    f"run is already in terminal state {run.state.value}",
                    details={"run_id": run_id, "state": run.state.value},
                )
            if run.state == RunState.QUEUED:
                # Terminal straight from the queue: no task may ever start.
                run.state = RunState.CANCELLED
                run.reason = "cancelled while queued"
                run.finished_at = _wall_now()
                for task in run.tasks.values():
                    task.state = TaskState.SKIPPED
                    task.error = "skipped: run cancelled while queued"
                    task.finished_at = _wall_now()
                run.cond.notify_all()
                log.info("queued run cancelled run=%s seq=%d", run_id, run.seq)
                return run
            self._teardown_run(run, "cancelled by user")
        log.info("run cancelled run=%s", run_id)
        return run

    def _teardown_run(self, run: Run, reason: str) -> None:
        """Stop every in-flight attempt and freeze not-started tasks.

        Called holding ``run.cond`` while the run is RUNNING. The scheduler
        converges the run to its terminal state once in-flight tasks settle.
        """
        run.reason = reason
        run.cancel_event.set()
        run.timed_out_event.set()
        for task in run.tasks.values():
            if task.state == TaskState.PENDING:
                task.state = TaskState.SKIPPED
                task.error = f"skipped: run {reason}"
                task.finished_at = _wall_now()
        run.cond.notify_all()

    def _wait_settled(self, run: Run, timeout: float = 30.0) -> None:
        """Block until every task has converged to a terminal state."""
        with run.cond:
            run.cond.wait_for(
                lambda: all(
                    task.state in _TERMINAL_TASK_STATES for task in run.tasks.values()
                ),
                timeout=timeout,
            )

    # -------------------------------------------------------------- admission
    def _drive(self, run: Run, definition: PipelineDef) -> None:
        admitted = False
        timer: threading.Timer | None = None
        try:
            with run.cond:
                if run.state != RunState.QUEUED:
                    return
            self._gate.acquire()
            admitted = True
            with run.cond:
                if run.state != RunState.QUEUED:
                    return
                run.state = RunState.RUNNING
                run.started_at = _wall_now()
                run.cond.notify_all()
            log.info("run admitted run=%s seq=%d", run.run_id, run.seq)
            if run.timeout_seconds is not None:
                timer = threading.Timer(
                    run.timeout_seconds, self._on_run_timeout, args=(run,)
                )
                timer.daemon = True
                timer.start()
            self._schedule(run, definition)
        except Exception:  # pragma: no cover - defensive
            log.exception("driver crashed run=%s", run.run_id)
            with run.cond:
                if run.state in (RunState.QUEUED, RunState.RUNNING):
                    self._teardown_run(run, "internal scheduler error")
        finally:
            if timer is not None:
                timer.cancel()
            if admitted:
                self._gate.release()

    def _on_run_timeout(self, run: Run) -> None:
        with run.cond:
            if run.state != RunState.RUNNING:
                return
            log.info(
                "run timed out run=%s timeout=%ss", run.run_id, run.timeout_seconds
            )
            self._teardown_run(run, f"run timed out after {run.timeout_seconds}s")

    # -------------------------------------------------------------- scheduling
    def _finalize(self, run: Run) -> None:
        """Decide the terminal run state. Caller holds run.cond; all tasks
        must already be terminal."""
        if run.state in _TERMINAL_RUN_STATES:
            return
        states = {t.state for t in run.tasks.values()}
        if run.reason is not None and run.cancel_event.is_set():
            if run.reason == "cancelled by user":
                run.state = RunState.CANCELLED
            else:
                run.state = RunState.FAILED
        elif TaskState.FAILED in states:
            run.state = RunState.FAILED
            if run.reason is None:
                run.reason = "a task failed after exhausting retries"
        else:
            run.state = RunState.SUCCEEDED
        run.finished_at = _wall_now()
        run.cond.notify_all()
        log.info(
            "run finished run=%s state=%s reason=%s",
            run.run_id,
            run.state.value,
            run.reason,
        )

    def _schedule(self, run: Run, definition: PipelineDef) -> None:
        in_flight: dict[str, Future] = {}
        remaining = set(definition.tasks)

        while True:
            ready: list[TaskDef] = []
            with run.cond:
                failed = any(
                    t.state == TaskState.FAILED for t in run.tasks.values()
                )
                tearing_down = run.cancel_event.is_set()
                if failed or tearing_down:
                    # Downstream tasks are forbidden from starting, but
                    # unrelated in-flight branches must converge first so the
                    # run never reports a terminal state while tasks RUNNING.
                    self._propagate_skips(run, definition)
                    if all(
                        task.state in _TERMINAL_TASK_STATES
                        for task in run.tasks.values()
                    ):
                        self._finalize(run)
                        return
                if not tearing_down and not failed:
                    for name in list(remaining):
                        task_def = definition.tasks[name]
                        if all(
                            run.tasks[d].state in _TERMINAL_TASK_STATES
                            for d in task_def.dependencies
                        ):
                            remaining.discard(name)
                            ready.append(task_def)
            for task_def in ready:
                in_flight[task_def.name] = self._task_pool.submit(
                    self._run_task, run, definition, task_def
                )
            if not in_flight:
                with run.cond:
                    if all(
                        task.state in _TERMINAL_TASK_STATES
                        for task in run.tasks.values()
                    ):
                        self._finalize(run)
                        return
            done = self._wait_any(run, in_flight)
            for name, future in done:
                in_flight.pop(name)
                future.result()  # surface unexpected worker errors

    @staticmethod
    def _wait_any(run: Run, in_flight: dict[str, Future]) -> list[tuple[str, Future]]:
        with run.cond:
            while run.state not in _TERMINAL_RUN_STATES:
                finished = [n for n, f in in_flight.items() if f.done()]
                if finished:
                    return [(n, in_flight[n]) for n in finished]
                run.cond.wait(timeout=0.05)
                finished = [n for n, f in in_flight.items() if f.done()]
                if finished:
                    return [(n, in_flight[n]) for n in finished]
        return [(n, f) for n, f in in_flight.items() if f.done()]

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
                    task.finished_at = _wall_now()
                    changed = True

    # ------------------------------------------------------------- task worker
    def _run_task(self, run: Run, definition: PipelineDef, task_def: TaskDef) -> None:
        task = run.tasks[task_def.name]
        max_attempts = task_def.max_retries + 1
        with run.cond:
            if run.state != RunState.RUNNING or task.state != TaskState.PENDING:
                return
            task.state = TaskState.RUNNING
            task.started_at = _wall_now()

        for attempt_no in range(1, max_attempts + 1):
            record = AttemptRecord(
                attempt=attempt_no, status="RUNNING", started_at=_wall_now()
            )
            task.attempts.append(record)
            # The attempt deadline event fires on task-level timeout; the
            # executor's sleep actions observe it so attempts abort promptly.
            attempt_event = threading.Event()
            attempt_timer: threading.Timer | None = None
            if task_def.timeout_seconds is not None:
                def _expire(event: threading.Event = attempt_event) -> None:
                    event.set()

                attempt_timer = threading.Timer(
                    task_def.timeout_seconds, _expire
                )
                attempt_timer.daemon = True
                attempt_timer.start()

            ctx = ExecutionContext(
                pipeline_name=definition.name,
                task_name=task_def.name,
                attempt=attempt_no,
                cancel_event=run.cancel_event,
                deadline_event=attempt_event,
            )
            log.info(
                "task attempt run=%s task=%s attempt=%d/%d timeout=%s action=%s",
                run.run_id,
                task_def.name,
                attempt_no,
                max_attempts,
                task_def.timeout_seconds,
                task_def.action.get("type", "succeed"),
            )
            failure: TaskFailure | None = None
            try:
                self.executor.execute(ctx, task_def.action)
            except TaskFailure as exc:
                failure = exc
            except Exception:  # pragma: no cover - defensive
                log.exception(
                    "task worker error run=%s task=%s", run.run_id, task_def.name
                )
                failure = TaskFailure("internal executor error")
            finally:
                if attempt_timer is not None:
                    attempt_timer.cancel()

            with run.cond:
                tearing_down = run.cancel_event.is_set()
                timed_out = attempt_event.is_set() and not tearing_down
                if tearing_down:
                    record.status = "FAILED" if failure is not None else "SUCCEEDED"
                    record.error = (
                        str(failure) if failure is not None else "completed at teardown"
                    )
                    record.finished_at = _wall_now()
                    task.state = TaskState.CANCELLED
                    task.error = run.reason or "run stopped before task converged"
                    task.finished_at = _wall_now()
                    run.cond.notify_all()
                    return
                if timed_out:
                    message = (
                        f"task attempt timed out after "
                        f"{task_def.timeout_seconds}s"
                    )
                    record.status = "FAILED"
                    record.error = message
                    record.finished_at = _wall_now()
                    log.warning(
                        "task attempt timed out run=%s task=%s attempt=%d",
                        run.run_id,
                        task_def.name,
                        attempt_no,
                    )
                    if attempt_no < max_attempts:
                        run.cond.notify_all()
                        continue
                    task.state = TaskState.FAILED
                    task.error = f"failed after {attempt_no} attempt(s): {message}"
                    task.finished_at = _wall_now()
                    run.cond.notify_all()
                    return
                if failure is not None:
                    record.status = "FAILED"
                    record.error = str(failure)
                    record.finished_at = _wall_now()
                    last_error = str(failure)
                    log.warning(
                        "task attempt failed run=%s task=%s attempt=%d error=%s",
                        run.run_id,
                        task_def.name,
                        attempt_no,
                        failure,
                    )
                    if attempt_no < max_attempts:
                        run.cond.notify_all()
                        continue
                    task.state = TaskState.FAILED
                    task.error = f"failed after {attempt_no} attempt(s): {last_error}"
                    task.finished_at = _wall_now()
                    run.cond.notify_all()
                    return
                record.status = "SUCCEEDED"
                record.finished_at = _wall_now()
                task.state = TaskState.SUCCEEDED
                task.finished_at = _wall_now()
                run.cond.notify_all()
            log.info(
                "task succeeded run=%s task=%s attempts=%d",
                run.run_id,
                task_def.name,
                attempt_no,
            )
            return
