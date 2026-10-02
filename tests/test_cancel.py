"""业务四：运行取消（未开始任务不再启动，进行中任务收敛到明确状态）。"""

from __future__ import annotations

import time

from app.models import RunState, TaskState
from tests.conftest import publish


def test_cancel_before_start_marks_all_tasks(orchestrator):
    publish(orchestrator, "chain", [("a", []), ("b", ["a"]), ("c", ["b"])])
    run, _ = orchestrator.trigger("chain")
    # 立即取消：所有任务收敛到 CANCELLED/SKIPPED 等明确终态，无 RUNNING/PENDING 残留
    orchestrator.cancel(run.run_id)

    view = orchestrator.wait_terminal(run.run_id)
    assert view.state in (RunState.CANCELLED, RunState.FAILED)
    states = {task.name: task.state for task in view.tasks}
    assert states["b"] in (TaskState.CANCELLED, TaskState.SKIPPED)
    assert states["c"] in (TaskState.CANCELLED, TaskState.SKIPPED)
    assert all(task.state in {
        TaskState.SUCCESS, TaskState.FAILED, TaskState.SKIPPED, TaskState.CANCELLED
    } for task in view.tasks)


def test_cancel_during_running_task_converges(orchestrator):
    publish(
        orchestrator,
        "pipeline",
        [
            ("a", []),
            ("b", ["a"]),
            ("c", ["b"]),
        ],
    )
    run, _ = orchestrator.trigger("pipeline", simulations={"a": {"duration_seconds": 0.3}})

    # 等待 a 进入 RUNNING 后取消
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        view = orchestrator.get_run(run.run_id).view()
        if view.tasks[0].state == TaskState.RUNNING:
            break
        time.sleep(0.01)
    orchestrator.cancel(run.run_id)

    view = orchestrator.wait_terminal(run.run_id)
    states = {task.name: task.state for task in view.tasks}
    # a 在执行中被取消，b/c 未开始不得启动
    assert states["a"] == TaskState.CANCELLED
    assert states["b"] in (TaskState.CANCELLED, TaskState.SKIPPED)
    assert states["c"] in (TaskState.CANCELLED, TaskState.SKIPPED)
    assert view.state in (RunState.CANCELLED, RunState.FAILED)
    assert view.finished_at is not None


def test_cancel_after_completion_is_rejected(orchestrator):
    from app.errors import DefinitionError, ErrorCode

    publish(orchestrator, "p", [("a", [])])
    run, _ = orchestrator.trigger("p")
    orchestrator.wait_terminal(run.run_id)

    try:
        orchestrator.cancel(run.run_id)
    except DefinitionError as exc:
        assert exc.code == ErrorCode.RUN_ALREADY_TERMINAL
    else:
        raise AssertionError("终态运行的取消应被拒绝")


def test_cancel_does_not_affect_other_runs(orchestrator):
    publish(orchestrator, "p", [("a", []), ("b", ["a"])])
    run1, _ = orchestrator.trigger("p", simulations={"a": {"duration_seconds": 0.25}})
    run2, _ = orchestrator.trigger("p")

    orchestrator.cancel(run1.run_id)

    v1 = orchestrator.wait_terminal(run1.run_id)
    v2 = orchestrator.wait_terminal(run2.run_id)
    assert v1.state in (RunState.CANCELLED, RunState.FAILED)
    assert v2.state == RunState.SUCCEEDED
