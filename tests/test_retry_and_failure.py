"""业务五：任务失败重试记录与失败传播（下游必须失败/跳过，不被计入成功）。"""

from __future__ import annotations

from app.models import AttemptStatus, RunState, TaskState
from tests.conftest import make_workflow, publish


def test_retry_records_every_attempt_and_eventually_succeeds(orchestrator):
    orchestrator.registry.publish(
        make_workflow(
            "retry-ok",
            [
                {"name": "a", "dependencies": [], "retries": 2, "retry_delay_seconds": 0},
            ],
        )
    )
    # 前两次失败、第三次成功：retries=2 共 3 次尝试
    run, _ = orchestrator.trigger(
        "retry-ok", simulations={"a": {"fail_times": 2, "duration_seconds": 0.01}}
    )
    view = orchestrator.wait_terminal(run.run_id)

    assert view.state == RunState.SUCCEEDED
    task = view.tasks[0]
    assert task.state == TaskState.SUCCESS
    statuses = [attempt.status for attempt in task.attempts]
    assert statuses == [AttemptStatus.FAILED, AttemptStatus.FAILED, AttemptStatus.SUCCESS]


def test_failure_after_retry_limit_propagates_downstream(orchestrator):
    orchestrator.registry.publish(
        make_workflow(
            "propagate",
            [
                {"name": "a", "dependencies": [], "retries": 1},
                {"name": "b", "dependencies": ["a"]},
                {"name": "c", "dependencies": ["b"]},
                {"name": "indep", "dependencies": []},
                {"name": "leaf", "dependencies": ["c", "indep"]},
            ],
        )
    )
    # a 的两次尝试都失败
    run, _ = orchestrator.trigger(
        "propagate", simulations={"a": {"fail_times": 5}}
    )
    view = orchestrator.wait_terminal(run.run_id)
    states = {task.name: task.state for task in view.tasks}

    assert view.state == RunState.FAILED
    assert states["a"] == TaskState.FAILED
    assert len(view.tasks[0].attempts) == 2
    assert states["b"] == TaskState.SKIPPED
    assert states["c"] == TaskState.SKIPPED
    assert states["indep"] == TaskState.SUCCESS
    assert states["leaf"] == TaskState.SKIPPED
    # 被跳过的任务一次都不允许执行
    by_name = {task.name: task for task in view.tasks}
    for skipped_name in ("b", "c", "leaf"):
        assert by_name[skipped_name].attempts == []


def test_no_retry_means_single_attempt(orchestrator):
    publish(orchestrator, "once", [("a", [])])
    run, _ = orchestrator.trigger("once", simulations={"a": {"fail_times": 1}})
    view = orchestrator.wait_terminal(run.run_id)
    assert view.state == RunState.FAILED
    assert view.tasks[0].state == TaskState.FAILED
    assert len(view.tasks[0].attempts) == 1


def test_already_succeeded_task_is_never_reexecuted(orchestrator):
    publish(orchestrator, "p", [("a", []), ("b", ["a"])])
    run, _ = orchestrator.trigger(
        "p", idempotency_key="stable", simulations={"a": {"fail_times": 0}}
    )
    orchestrator.wait_terminal(run.run_id)

    # 再次以相同幂等键触发，返回同一运行且 a/b 仍只有一次成功尝试
    same_run, dedup = orchestrator.trigger("p", idempotency_key="stable")
    assert dedup is True
    view = orchestrator.wait_terminal(same_run.run_id)
    for task in view.tasks:
        assert task.state == TaskState.SUCCESS
        assert len(task.attempts) == 1


def test_diamond_failure_marks_only_affected_branch(orchestrator):
    publish(
        orchestrator,
        "diamond",
        [
            ("root", []),
            ("bad", ["root"]),
            ("good", ["root"]),
            ("merge", ["bad", "good"]),
        ],
    )
    run, _ = orchestrator.trigger("diamond", simulations={"bad": {"fail_times": 1}})
    view = orchestrator.wait_terminal(run.run_id)
    states = {task.name: task.state for task in view.tasks}

    assert states["root"] == TaskState.SUCCESS
    assert states["bad"] == TaskState.FAILED
    assert states["good"] == TaskState.SUCCESS
    assert states["merge"] == TaskState.SKIPPED
