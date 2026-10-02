"""业务二：运行触发与幂等（同一幂等标识重复触发只能产生一个运行）。"""

from __future__ import annotations

import threading

from app.models import RunState, TaskState
from tests.conftest import publish


def test_simple_run_succeeds_in_dependency_order(orchestrator):
    publish(orchestrator, "p", [("a", []), ("b", ["a"]), ("c", ["b"])])
    run, deduplicated = orchestrator.trigger("p")
    assert deduplicated is False

    view = orchestrator.wait_terminal(run.run_id)
    assert view.state == RunState.SUCCEEDED
    assert [task.state for task in view.tasks] == [
        TaskState.SUCCESS,
        TaskState.SUCCESS,
        TaskState.SUCCESS,
    ]
    for task in view.tasks:
        assert len(task.attempts) == 1 and task.attempts[0].status.value == "SUCCESS"


def test_idempotent_retrigger_returns_same_run(orchestrator):
    publish(orchestrator, "p", [("a", [])])
    first, dedup1 = orchestrator.trigger("p", idempotency_key="key-1")
    second, dedup2 = orchestrator.trigger("p", idempotency_key="key-1")

    assert dedup1 is False and dedup2 is True
    assert first.run_id == second.run_id
    view = orchestrator.wait_terminal(first.run_id)
    assert view.idempotency_key == "key-1"
    assert view.tasks[0].state == TaskState.SUCCESS
    assert len(view.tasks[0].attempts) == 1


def test_different_idempotency_keys_create_separate_runs(orchestrator):
    publish(orchestrator, "p", [("a", [])])
    run1, _ = orchestrator.trigger("p", idempotency_key="k1")
    run2, _ = orchestrator.trigger("p", idempotency_key="k2")
    assert run1.run_id != run2.run_id
    assert orchestrator.wait_terminal(run1.run_id).state == RunState.SUCCEEDED
    assert orchestrator.wait_terminal(run2.run_id).state == RunState.SUCCEEDED


def test_concurrent_triggers_with_same_key_deduplicate(orchestrator):
    publish(
        orchestrator,
        "slow",
        [
            {"name": "a", "dependencies": [], "retries": 0},
        ],
    )
    # 给任务注入耗时，放大触发竞态窗口
    sims = {"a": {"duration_seconds": 0.15}}
    results: list[tuple[str, bool]] = []
    barrier = threading.Barrier(5)

    def fire() -> None:
        barrier.wait()
        run, dedup = orchestrator.trigger("slow", idempotency_key="race", simulations=sims)
        results.append((run.run_id, dedup))

    threads = [threading.Thread(target=fire) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    run_ids = {run_id for run_id, _ in results}
    assert len(run_ids) == 1
    assert sum(1 for _, dedup in results if dedup is False) == 1
    assert sum(1 for _, dedup in results if dedup is True) == 4
    run_id = results[0][0]
    assert orchestrator.wait_terminal(run_id).state == RunState.SUCCEEDED


def test_version_pinned_at_trigger_time(orchestrator):
    publish(orchestrator, "p", [("a", [])])
    publish(orchestrator, "p", [("a", []), ("b", ["a"])])

    old_run, _ = orchestrator.trigger("p", version=1)
    new_run, _ = orchestrator.trigger("p")  # 默认最新版本 v2

    old_view = orchestrator.wait_terminal(old_run.run_id)
    new_view = orchestrator.wait_terminal(new_run.run_id)
    assert old_view.definition_version == 1 and len(old_view.tasks) == 1
    assert new_view.definition_version == 2 and len(new_view.tasks) == 2


def test_completed_run_is_not_re_executed_by_retrigger(orchestrator):
    publish(orchestrator, "p", [("a", [])])
    first, _ = orchestrator.trigger("p", idempotency_key="once")
    orchestrator.wait_terminal(first.run_id)
    second, dedup = orchestrator.trigger("p", idempotency_key="once")

    assert dedup is True and second.run_id == first.run_id
    view = orchestrator.wait_terminal(second.run_id)
    assert len(view.tasks[0].attempts) == 1
