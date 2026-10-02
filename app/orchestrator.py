"""运行编排：依赖调度、并发运行、幂等触发、取消、重试与失败传播。"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone

from app.definition import WorkflowDefinition, WorkflowRegistry
from app.errors import DefinitionError, ErrorCode
from app.executor import (
    CancellationRequested,
    LocalTaskExecutor,
    SimulatedFailure,
)
from app.models import (
    Attempt,
    AttemptStatus,
    RunState,
    RunView,
    TaskRuntime,
    TaskState,
    TERMINAL_RUN_STATES,
    TERMINAL_TASK_STATES,
)

logger = logging.getLogger("pipeline.orchestrator")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RunContext:
    def __init__(
        self,
        run_id: str,
        definition: WorkflowDefinition,
        idempotency_key: str | None,
        simulations: dict[str, dict[str, float | int]] | None,
    ) -> None:
        self.run_id = run_id
        self.definition = definition
        self.idempotency_key = idempotency_key
        self.simulations = simulations or {}
        self.state = RunState.PENDING
        self.cancel_event = threading.Event()
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.created_at = _now()
        self.finished_at: datetime | None = None
        self.tasks: dict[str, TaskRuntime] = {
            task.name: TaskRuntime(name=task.name, state=TaskState.PENDING)
            for task in definition.tasks
        }

    def view(self) -> RunView:
        with self.lock:
            return RunView(
                run_id=self.run_id,
                workflow_name=self.definition.name,
                definition_version=self.definition.version,
                state=self.state,
                idempotency_key=self.idempotency_key,
                created_at=self.created_at,
                finished_at=self.finished_at,
                tasks=[self.tasks[task.name].model_copy(deep=True) for task in self.definition.tasks],
            )


class Orchestrator:
    def __init__(
        self,
        registry: WorkflowRegistry | None = None,
        task_executor: LocalTaskExecutor | None = None,
        poll_interval: float = 0.02,
    ) -> None:
        self.registry = registry or WorkflowRegistry()
        self.executor = task_executor or LocalTaskExecutor()
        self.poll_interval = poll_interval
        self.runs: dict[str, RunContext] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self.master_lock = threading.RLock()
        self._changed = threading.Condition(self.master_lock)
        self._stop = threading.Event()
        self._dispatcher: threading.Thread | None = None
        self._workers: set[threading.Thread] = set()

    def start(self) -> None:
        if self._dispatcher is not None:
            return
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop, name="dispatcher", daemon=True
        )
        self._dispatcher.start()

    def shutdown(self) -> None:
        self._stop.set()
        with self._changed:
            self._changed.notify_all()
        for run in list(self.runs.values()):
            run.cancel_event.set()

    def trigger(
        self,
        workflow_name: str,
        idempotency_key: str | None = None,
        version: int | None = None,
        simulations: dict[str, dict[str, float | int]] | None = None,
    ) -> tuple[RunContext, bool]:
        """触发运行，返回 (运行上下文, 是否为幂等去重命中)。"""
        definition = self.registry.get(workflow_name, version)
        with self.master_lock:
            if idempotency_key is not None:
                existing_id = self._idempotency.get((workflow_name, idempotency_key))
                if existing_id is not None:
                    logger.info(
                        "幂等命中 workflow=%s key=%s run_id=%s",
                        workflow_name,
                        idempotency_key,
                        existing_id,
                    )
                    return self.runs[existing_id], True

            run_id = uuid.uuid4().hex[:12]
            run = RunContext(run_id, definition, idempotency_key, simulations)
            self.runs[run_id] = run
            if idempotency_key is not None:
                self._idempotency[(workflow_name, idempotency_key)] = run_id

        logger.info(
            "触发运行 run_id=%s workflow=%s version=%s key=%s tasks=%d",
            run_id,
            workflow_name,
            definition.version,
            idempotency_key,
            len(definition.tasks),
        )
        self._wake()
        return run, False

    def get_run(self, run_id: str) -> RunContext:
        with self.master_lock:
            run = self.runs.get(run_id)
        if run is None:
            raise DefinitionError(
                ErrorCode.UNKNOWN_WORKFLOW,
                f"运行 '{run_id}' 不存在",
                run_id=run_id,
            )
        return run

    def cancel(self, run_id: str) -> RunContext:
        run = self.get_run(run_id)
        with run.lock:
            if run.state in TERMINAL_RUN_STATES:
                raise DefinitionError(
                    ErrorCode.RUN_ALREADY_TERMINAL,
                    f"运行 '{run_id}' 已处于终态 {run.state.value}，无法取消",
                    run_id=run_id,
                    state=run.state.value,
                )
            run.state = RunState.CANCELLING
            run.cancel_event.set()
        logger.info("请求取消 run_id=%s", run_id)
        self._wake()
        return run

    def wait_terminal(self, run_id: str, timeout: float | None = 10.0) -> RunView:
        run = self.get_run(run_id)
        with run.condition:
            run.condition.wait_for(lambda: run.state in TERMINAL_RUN_STATES, timeout=timeout)
            return run.view()

    def _wake(self) -> None:
        with self._changed:
            self._changed.notify_all()

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            with self.master_lock:
                runs = list(self.runs.values())
            for run in runs:
                try:
                    self._schedule(run)
                except Exception:  # noqa: BLE001 - 调度线程不能因单次异常退出
                    logger.exception("调度异常 run_id=%s", run.run_id)
            with self._changed:
                self._changed.wait(timeout=self.poll_interval)

    def _schedule(self, run: RunContext) -> None:
        spawned: list[str] = []
        with run.lock:
            if run.state in TERMINAL_RUN_STATES:
                return

            for task in run.definition.tasks:
                runtime = run.tasks[task.name]
                if runtime.state != TaskState.PENDING:
                    continue
                dep_states = [run.tasks[dep].state for dep in task.dependencies]
                if any(s == TaskState.FAILED for s in dep_states):
                    runtime.state = TaskState.SKIPPED
                    runtime.reason = "上游任务失败，依赖链跳过"
                elif any(s in (TaskState.SKIPPED, TaskState.CANCELLED) for s in dep_states):
                    runtime.state = TaskState.SKIPPED
                    runtime.reason = "上游任务未成功完成，依赖链跳过"

            cancelling = run.cancel_event.is_set()
            for task in run.definition.tasks:
                runtime = run.tasks[task.name]
                if runtime.state != TaskState.PENDING:
                    continue
                if cancelling:
                    runtime.state = TaskState.CANCELLED
                    runtime.reason = "运行取消时任务尚未开始"
                    continue
                dep_states = [run.tasks[dep].state for dep in task.dependencies]
                if all(s == TaskState.SUCCESS for s in dep_states):
                    runtime.state = TaskState.RUNNING
                    spawned.append(task.name)

            if any(rt.state == TaskState.RUNNING for rt in run.tasks.values()):
                run.state = RunState.RUNNING

            self._finalize_if_done(run)

        for task_name in spawned:
            worker = threading.Thread(
                target=self._run_task,
                args=(run, task_name),
                name=f"worker-{run.run_id}-{task_name}",
                daemon=True,
            )
            self._workers.add(worker)
            worker.start()
            logger.info("启动任务 run_id=%s task=%s worker=%s", run.run_id, task_name, worker.name)

        # 回收已结束的 worker 线程引用
        self._workers = {thread for thread in self._workers if thread.is_alive()}

    def _finalize_if_done(self, run: RunContext) -> None:
        """调用方必须持有 run.lock。"""
        states = [runtime.state for runtime in run.tasks.values()]
        if any(s not in TERMINAL_TASK_STATES for s in states):
            return

        if TaskState.FAILED in states:
            new_state = RunState.FAILED
        elif run.cancel_event.is_set():
            new_state = RunState.CANCELLED
        elif TaskState.SKIPPED in states or TaskState.CANCELLED in states:
            new_state = RunState.FAILED
        else:
            new_state = RunState.SUCCEEDED

        run.state = new_state
        run.finished_at = _now()
        run.condition.notify_all()
        logger.info(
            "运行终态 run_id=%s state=%s tasks=%s",
            run.run_id,
            new_state.value,
            {name: rt.state.value for name, rt in run.tasks.items()},
        )

    def _run_task(self, run: RunContext, task_name: str) -> None:
        definition = run.definition.tasks_by_name[task_name]
        max_attempts = definition.retries + 1
        try:
            for attempt_no in range(1, max_attempts + 1):
                started_at = _now()
                with run.lock:
                    run.tasks[task_name].state = TaskState.RUNNING

                try:
                    self.executor.execute(
                        task_name,
                        attempt_no,
                        run.simulations.get(task_name),
                        run.cancel_event,
                    )
                except CancellationRequested:
                    with run.lock:
                        self._append_attempt(
                            run, task_name, attempt_no, AttemptStatus.CANCELLED,
                            "执行过程中收到取消信号", started_at,
                        )
                        if run.tasks[task_name].state == TaskState.RUNNING:
                            run.tasks[task_name].state = TaskState.CANCELLED
                            run.tasks[task_name].reason = "执行过程中被取消"
                    return
                except SimulatedFailure as exc:
                    with run.lock:
                        self._append_attempt(
                            run, task_name, attempt_no, AttemptStatus.FAILED, str(exc), started_at
                        )
                        cancelled = run.cancel_event.is_set()

                    logger.warning(
                        "任务尝试失败 run_id=%s task=%s attempt=%d/%d cancelled=%s",
                        run.run_id, task_name, attempt_no, max_attempts, cancelled,
                    )

                    if cancelled:
                        with run.lock:
                            if run.tasks[task_name].state == TaskState.RUNNING:
                                run.tasks[task_name].state = TaskState.CANCELLED
                                run.tasks[task_name].reason = "失败后收敛于取消"
                        return
                    if attempt_no < max_attempts:
                        if self.executor.sleep_cancellable(
                            definition.retry_delay_seconds, run.cancel_event
                        ):
                            with run.lock:
                                if run.tasks[task_name].state == TaskState.RUNNING:
                                    run.tasks[task_name].state = TaskState.CANCELLED
                                    run.tasks[task_name].reason = "重试等待期间被取消"
                            return
                        continue

                    with run.lock:
                        if run.tasks[task_name].state == TaskState.RUNNING:
                            run.tasks[task_name].state = TaskState.FAILED
                            run.tasks[task_name].reason = (
                                f"重试上限耗尽（{max_attempts} 次尝试均失败）"
                            )
                    return
                else:
                    with run.lock:
                        self._append_attempt(
                            run, task_name, attempt_no, AttemptStatus.SUCCESS, None, started_at
                        )
                        if run.tasks[task_name].state == TaskState.RUNNING:
                            run.tasks[task_name].state = TaskState.SUCCESS
                    logger.info(
                        "任务成功 run_id=%s task=%s attempt=%d",
                        run.run_id, task_name, attempt_no,
                    )
                    return
        finally:
            self._wake()

    @staticmethod
    def _append_attempt(
        run: RunContext,
        task_name: str,
        attempt_no: int,
        status: AttemptStatus,
        error: str | None,
        started_at: datetime,
    ) -> None:
        run.tasks[task_name].attempts.append(
            Attempt(
                attempt=attempt_no,
                status=status,
                error=error,
                started_at=started_at,
                finished_at=_now(),
            )
        )
