"""Run admission, scheduling, timeouts, idempotency and retry propagation."""

from __future__ import annotations

import collections
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from .errors import (
    RunNotCancellableError,
    RunNotFoundError,
)
from .executor import (
    ExecutionContext,
    LocalExecutor,
    TaskFailure,
    TaskTimeoutError,
    now,
)
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


class Orchestrator:
    def __init__(
        self,
        registry: DefinitionRegistry,
        executor: LocalExecutor | None = None,
        max_concurrent_runs: int = 8,
        max_task_workers: int | None = None,
    ) -> None:
        self.registry = registry
        self.executor = executor or LocalExecutor()
        if max_concurrent_runs < 1:
            raise ValueError("max_concurrent_runs must be >= 1")
        self.max_concurrent_runs = max_concurrent_runs
        # One scheduler future per admitted run, plus task workers. Capacity is
        # enforced by admission, so the task pool only needs to be large enough
        # that admitted runs never block each other: worst case every run fans
        # out; cap the headroom to keep threads bounded.
        max_task_workers = max_task_workers or min(64, max(16, max_concurrent_runs * 8))
        self._sched_pool = ThreadPoolExecutor(
            max_workers=max_concurrent_runs, thread_name_prefix="pipe-run"
        )
        self._task_pool = ThreadPoolExecutor(
            max_workers=max_task_workers, thread_name_prefix="pipe-task"
        )
        self._runs: dict[str, Run] = {}
        # (pipeline, idempotency_key) -> run_id; a run owns its key forever,
        # including after it finishes, so late re-triggers return the result.
        self._idempotency: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()
        # Admission control: FIFO queue of (run, definition snapshot) waiting.
        self._queue: collections.deque[tuple[Run, PipelineDef]] = collections.deque()
        self._active = 0
        self._admission_cond = threading.Condition(self._lock)
        self._sequence = 0
        self._stopping = False
        self._dispatcher = threading.Thread(
            target=self._admit_loop, name="pipe-admit", daemon=True
        )
        self._dispatcher.start()

    def shutdown(self) -> None:
        with self._admission_cond:
            self._stopping = True
            self._admission_cond.notify_all()
        self._sched_pool.shutdown(wait=True, cancel_futures=False)
        self._task_pool.shutdown(wait=True, cancel_futures=False)

    # ------------------------------------------------------------------ runs
    def start_run(
        self,
        pipeline_name: str,
        *,
        version: int | None = None,
        idempotency_key: str | None = None,
        run_timeout_seconds: float | None = None,
    ) -> tuple[Run, bool]:
        """Start (or return the existing run for) a pipeline run.

        Returns ``(run, created)``. The definition snapshot is captured here,
        so later submissions of new versions never affect this run.

        New runs enter QUEUED and start in trigger order once capacity frees
        up; ``run_timeout_seconds`` bounds the whole run from the moment it is
        admitted out of the queue.
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
            run.timeout_seconds = run_timeout_seconds
            self._sequence += 1
            run.sequence = self._sequence
            self._runs[run.run_id] = run
            if idempotency_key is not None:
                self._idempotency[(definition.name, idempotency_key)] = run.run_id
            self._queue.append((run, definition))
            self._admission_cond.notify()
        log.info(
            "run queued pipeline=%s version=%d run=%s tasks=%d key=%s",
            run.pipeline_name,
            run.version,
            run.run_id,
            len(run.tasks),
            idempotency_key,
        )
        return run, True

    # --------------------------------------------------------- admission gate
    def _admit_loop(self) -> None:
        while True:
            admitted: list[tuple[Run, PipelineDef]] = []
            with self._admission_cond:
                while not self._stopping and not (
                    self._queue and self._active < self.max_concurrent_runs
                ):
                    self._admission_cond.wait(timeout=0.1)
                    if self._stopping:
                        break
                if self._stopping:
                    return
                while self._queue and self._active < self.max_concurrent_runs:
                    run, definition = self._queue.popleft()
                    # A run can leave the queue only by admission or by being
                    # cancelled while queued (which removes it explicitly);
                    # guard anyway so a stale entry never steals a slot.
                    if run.state != RunState.QUEUED:
                        continue
                    self._active += 1
                    admitted.append((run, definition))
            for run, definition in admitted:
                self._mark_admitted(run)
                if run.state == RunState.RUNNING:
                    self._sched_pool.submit(self._schedule, run, definition)
                else:
                    # Cancelled in the tiny window between selection and the
                    # state flip: it never owned a scheduler slot.
                    self._release_slot()

    @staticmethod
    def _mark_admitted(run: Run) -> None:
        with run.cond:
            if run.state != RunState.QUEUED:
                return
            run.state = RunState.RUNNING
            run.started_at = now()
            run.wall_started_at = time.time()
            run.cond.notify_all()

    def _release_slot(self) -> None:
        with self._admission_cond:
            self._active -= 1
            self._admission_cond.notify()

    def get_run(self, run_id: str) -> Run:
        with self._lock:
            run = self._runs.get(run_id)
        if run is None:
            raise RunNotFoundError(
                f"run '{run_id}' does not exist", details={"run_id": run_id}
            )
        return run

    def get_run_snapshot(self, run_id: str) -> Run:
        """Read-side accessor: always returns a detached consistent copy."""
        return self.get_run(run_id).snapshot()

    def list_runs(self, pipeline_name: str | None = None) -> list[Run]:
        with self._lock:
            runs = list(self._runs.values())
        if pipeline_name is not None:
            runs = [r for r in runs if r.pipeline_name == pipeline_name]
        return [run.snapshot() for run in runs]

    def query_runs(
        self,
        *,
        pipeline_name: str | None = None,
        state: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Run], int]:
        """Filtered, paginated history ordered by trigger sequence.

        Filtering, total counting and paging happen on one consistent
        snapshot, so a page can never duplicate or drop a run. Detached
        copies are returned, so serialization never blocks live scheduling.
        """
        states = self._parse_states(state)
        with self._lock:
            matched = [
                run
                for run in self._runs.values()
                if self._matches(
                    run, pipeline_name, states, start_time, end_time
                )
            ]
            matched.sort(key=lambda r: (r.sequence, r.run_id))
            total = len(matched)
            page = matched[offset : offset + limit]
        return [run.snapshot() for run in page], total

    def run_stats(
        self,
        *,
        pipeline_name: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> dict:
        """Per-state counts plus queue depth and average finished duration.

        Uses the same filters (minus state) as ``query_runs`` so statistics
        and listed details always reconcile.
        """
        counts = {s.value: 0 for s in RunState}
        durations: list[float] = []
        with self._lock:
            for run in list(self._runs.values()):
                if pipeline_name is not None and run.pipeline_name != pipeline_name:
                    continue
                if start_time is not None and run.wall_created_at < start_time:
                    continue
                if end_time is not None and run.wall_created_at > end_time:
                    continue
                counts[run.state.value] += 1
                if (
                    run.state in _TERMINAL_RUN_STATES
                    and run.wall_started_at is not None
                    and run.wall_finished_at is not None
                ):
                    durations.append(
                        run.wall_finished_at - run.wall_started_at
                    )
        finished = (
            counts[RunState.SUCCEEDED.value]
            + counts[RunState.FAILED.value]
            + counts[RunState.CANCELLED.value]
        )
        return {
            "states": counts,
            "total": sum(counts.values()),
            "queued": counts[RunState.QUEUED.value],
            "running": counts[RunState.RUNNING.value],
            "finished": finished,
            "average_duration_seconds": (
                sum(durations) / len(durations) if durations else None
            ),
        }

    @staticmethod
    def _parse_states(state: str | None) -> set[RunState] | None:
        if state is None:
            return None
        names = {p.strip().upper() for p in state.split(",") if p.strip()}
        invalid = names - {s.value for s in RunState}
        if invalid:
            from .errors import InvalidQueryError

            raise InvalidQueryError(
                f"unknown run state filter: {', '.join(sorted(invalid))}",
                details={"invalid": sorted(invalid)},
            )
        return {RunState(n) for n in names}

    @staticmethod
    def _matches(
        run: Run,
        pipeline_name: str | None,
        states: set[RunState] | None,
        start_time: float | None,
        end_time: float | None,
    ) -> bool:
        if pipeline_name is not None and run.pipeline_name != pipeline_name:
            return False
        if states is not None and run.state not in states:
            return False
        if start_time is not None and run.wall_created_at < start_time:
            return False
        if end_time is not None and run.wall_created_at > end_time:
            return False
        return True

    def wait_for(self, run_id: str, timeout: float | None = None) -> Run:
        run = self.get_run(run_id)
        with run.cond:
            if run.state not in _TERMINAL_RUN_STATES:
                run.cond.wait_for(
                    lambda: run.state in _TERMINAL_RUN_STATES, timeout=timeout
                )
        return run.snapshot()

    # ------------------------------------------------------------ cancellation
    def cancel_run(self, run_id: str) -> Run:
        run = self.get_run(run_id)
        queued = False
        with self._lock:
            if run.state == RunState.QUEUED:
                for item in list(self._queue):
                    if item[0] is run:
                        self._queue.remove(item)
                        queued = True
                        break
        self._abort_run(
            run,
            RunState.CANCELLED,
            "cancelled while queued: no task was started" if queued
            else "run cancelled",
        )
        log.info("run cancelled run=%s queued=%s", run_id, queued)
        return run

    def _abort_run(self, run: Run, final_state: RunState, reason: str) -> None:
        """Move a QUEUED run to a terminal state, or request convergence of a
        RUNNING run.

        For a running run the terminal state is *not* published while tasks
        are still executing. Instead the abort request is recorded, every
        not-yet-started task becomes SKIPPED, and the scheduler lands the
        terminal state only once all in-flight tasks have settled. Readers
        therefore keep seeing RUNNING until the run is self-consistent.
        """
        with run.cond:
            if run.state in _TERMINAL_RUN_STATES or run.abort_target is not None:
                raise RunNotCancellableError(
                    f"run is already {run.state.value}"
                    + (
                        f" (aborting to {run.abort_target.value})"
                        if run.abort_target is not None
                        else ""
                    ),
                    details={
                        "run_id": run.run_id,
                        "state": run.state.value,
                    },
                )
            from_queue = run.state == RunState.QUEUED
            run.cancel_event.set()
            if from_queue:
                run.state = final_state
                run.failure_reason = reason
                for task in run.tasks.values():
                    if task.state == TaskState.PENDING:
                        task.state = TaskState.SKIPPED
                        task.error = (
                            "skipped: run cancelled while queued"
                            if final_state == RunState.CANCELLED
                            else f"skipped: run {final_state.value.lower()} before start"
                        )
                        task.finished_at = now()
                run.started_at = None
                run.wall_started_at = None
                run.finished_at = None
                run.wall_finished_at = time.time()
                run.cond.notify_all()
            else:
                # Defer the visible terminal state until convergence.
                run.abort_target = final_state
                run.abort_reason = reason
                for task in run.tasks.values():
                    if task.state == TaskState.PENDING:
                        task.state = TaskState.SKIPPED
                        task.error = (
                            f"skipped: run {final_state.value.lower()} before start"
                        )
                        task.finished_at = now()
                run.cond.notify_all()
        if from_queue:
            with self._admission_cond:
                self._admission_cond.notify()

    def _on_run_timeout(self, run: Run) -> None:
        with run.cond:
            if (
                run.state != RunState.RUNNING
                or run.abort_target is not None
            ):
                return
            seconds = run.timeout_seconds
            reason = (
                f"run exceeded timeout of {seconds:.3f}s"
                if seconds is not None
                else "run timed out"
            )
        try:
            self._abort_run(run, RunState.FAILED, reason)
        except RunNotCancellableError:
            return
        log.warning("run timeout requested run=%s", run.run_id)

    # -------------------------------------------------------------- scheduling
    def _schedule(self, run: Run, definition: PipelineDef) -> None:
        in_flight: dict[str, Future] = {}
        remaining = set(definition.tasks)
        timer: threading.Timer | None = None

        if run.timeout_seconds is not None and run.state == RunState.RUNNING:
            timer = threading.Timer(
                run.timeout_seconds, self._on_run_timeout, args=(run,)
            )
            timer.daemon = True
            timer.start()

        try:
            while True:
                ready: list[TaskDef] = []
                with run.cond:
                    abort = run.abort_target
                    if run.state != RunState.RUNNING:
                        break
                    if abort is not None:
                        # A cancel/timeout was requested: no task may start,
                        # no retry may be issued and no downstream unlocks.
                        # Just drain the in-flight set, then land the
                        # terminal state atomically with task convergence.
                        if not in_flight:
                            self._finish_locked(run, abort, run.abort_reason)
                            break
                    else:
                        failed = any(
                            t.state == TaskState.FAILED
                            for t in run.tasks.values()
                        )
                        if failed:
                            # Downstream tasks are forbidden from starting,
                            # but unrelated in-flight branches must converge
                            # first so the run never reports FAILED while
                            # tasks are RUNNING.
                            self._propagate_skips(run, definition)
                            if all(
                                task.state in _TERMINAL_TASK_STATES
                                for task in run.tasks.values()
                            ):
                                self._finish_locked(run, RunState.FAILED)
                                break
                        # Launch tasks whose dependencies are settled.
                        # Tasks flipped to SKIPPED by an abort are skipped
                        # here, so an abort never launches a new task.
                        for name in list(remaining):
                            task_def = definition.tasks[name]
                            if run.tasks[name].state != TaskState.PENDING:
                                remaining.discard(name)
                                continue
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
                    # Nothing running and nothing newly launched means all
                    # tasks reached a terminal state without failures.
                    break
                finished, _ = wait(
                    list(in_flight.values()),
                    timeout=0.1,
                    return_when=FIRST_COMPLETED,
                )
                for future in finished:
                    name = next(n for n, f in in_flight.items() if f is future)
                    in_flight.pop(name)
                    future.result()  # surface unexpected worker errors
            with run.cond:
                if run.state == RunState.RUNNING:
                    self._finish_locked(run, RunState.SUCCEEDED)
        except Exception:  # pragma: no cover - defensive
            log.exception("scheduler crashed run=%s", run.run_id)
            with run.cond:
                if run.state == RunState.RUNNING:
                    self._finish_locked(
                        run, RunState.FAILED, "internal scheduler error"
                    )
        finally:
            if timer is not None:
                timer.cancel()
            # When cancelled, in-flight tasks may still be executing; wait for
            # them to converge so status is never self-contradictory.
            for future in list(in_flight.values()):
                future.result()
            self._release_slot()

    @staticmethod
    def _finish_locked(
        run: Run, state: RunState, reason: str | None = None
    ) -> None:
        run.state = state
        if reason is not None:
            run.failure_reason = reason
        run.finished_at = now()
        run.wall_finished_at = time.time()
        run.cond.notify_all()

    def _wait_settled(self, run: Run, timeout: float = 30.0) -> None:
        """Block until the run is terminal, which is published only after
        every task has converged to a terminal state."""
        with run.cond:
            deadline = time.monotonic() + timeout
            while run.state not in _TERMINAL_RUN_STATES:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                run.cond.wait(timeout=remaining)

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
            if (
                run.state != RunState.RUNNING
                or run.abort_target is not None
                or task.state != TaskState.PENDING
            ):
                return
            task.state = TaskState.RUNNING
            task.started_at = now()

        for attempt_no in range(1, max_attempts + 1):
            record = AttemptRecord(
                attempt=attempt_no, status="RUNNING", started_at=now()
            )
            task.attempts.append(record)
            deadline = (
                now() + task_def.timeout_seconds
                if task_def.timeout_seconds is not None
                else None
            )
            ctx = ExecutionContext(
                pipeline_name=definition.name,
                task_name=task_def.name,
                attempt=attempt_no,
                cancel_event=run.cancel_event,
                deadline=deadline,
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
                # An action that ignores its deadline must never look like a
                # success: a timeout is an attempt failure, full stop.
                if deadline is not None and now() > deadline:
                    raise TaskTimeoutError(
                        f"task '{task_def.name}' attempt timed out after "
                        f"{task_def.timeout_seconds:.3f}s"
                    )
            except TaskFailure as exc:
                timed_out = isinstance(exc, TaskTimeoutError)
                record.status = "FAILED"
                record.error = str(exc)
                record.finished_at = now()
                last_error = str(exc)
                log.warning(
                    "task attempt %s run=%s task=%s attempt=%d error=%s",
                    "timed out" if timed_out else "failed",
                    run.run_id,
                    task_def.name,
                    attempt_no,
                    exc,
                )
                with run.cond:
                    # The run may have been aborted (cancel/timeout) while we
                    # were executing; re-check under the lock. The timed-out
                    # attempt is already booked FAILED above and is never
                    # retried.
                    abort = run.abort_target
                    if run.state != RunState.RUNNING or abort is not None:
                        task.state = (
                            TaskState.CANCELLED
                            if abort == RunState.CANCELLED
                            else TaskState.FAILED
                        )
                        task.error = (
                            f"{run.abort_reason or run.failure_reason or 'run aborted'}"
                            f": {last_error}"
                        )
                        task.finished_at = now()
                        run.cond.notify_all()
                        return
                if attempt_no < max_attempts:
                    continue
                with run.cond:
                    task.state = TaskState.FAILED
                    prefix = (
                        "timed out"
                        if timed_out
                        else "failed"
                    )
                    task.error = (
                        f"{prefix} after {attempt_no} attempt(s): {last_error}"
                    )
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
                    abort = run.abort_target
                    if run.state != RunState.RUNNING or abort is not None:
                        # Finished at the instant the run was aborted (or
                        # while it was converging). Do not book it as
                        # success against an aborted run; converge to the
                        # state matching the abort cause.
                        record.status = "FAILED"
                        record.error = (
                            run.abort_reason or run.failure_reason or "run aborted"
                        )
                        record.finished_at = now()
                        task.state = (
                            TaskState.CANCELLED
                            if abort == RunState.CANCELLED
                            else TaskState.FAILED
                        )
                        task.error = "aborted after last attempt finished: " + (
                            run.abort_reason or run.failure_reason or "run aborted"
                        )
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
