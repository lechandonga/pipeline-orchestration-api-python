"""业务三：同一流程的多个并发运行相互独立、按依赖推进。"""

from __future__ import annotations

from app.models import RunState
from tests.conftest import publish


def test_multiple_concurrent_runs_are_independent(orchestrator):
    publish(
        orchestrator,
        "diamond",
        [
            ("root", []),
            ("left", ["root"]),
            ("right", ["root"]),
            ("leaf", ["left", "right"]),
        ],
    )

    # 第一个运行注入失败，第二个运行正常；并发运行结论互不影响
    run_fail, _ = orchestrator.trigger(
        "diamond",
        simulations={"left": {"fail_times": 1, "duration_seconds": 0.02}},
    )
    run_ok, _ = orchestrator.trigger("diamond")

    fail_view = orchestrator.wait_terminal(run_fail.run_id, timeout=10)
    ok_view = orchestrator.wait_terminal(run_ok.run_id, timeout=10)

    assert fail_view.run_id != ok_view.run_id
    assert fail_view.state == RunState.FAILED
    assert ok_view.state == RunState.SUCCEEDED


def test_independent_tasks_run_concurrently(orchestrator):
    publish(
        orchestrator,
        "parallel",
        [
            ("a", []),
            ("b", []),
            ("c", []),
        ],
    )
    sims = {
        "a": {"duration_seconds": 0.2},
        "b": {"duration_seconds": 0.2},
        "c": {"duration_seconds": 0.2},
    }
    import time

    run, _ = orchestrator.trigger("parallel", simulations=sims)
    start = time.monotonic()
    view = orchestrator.wait_terminal(run.run_id, timeout=10)
    elapsed = time.monotonic() - start

    assert view.state == RunState.SUCCEEDED
    # 三个任务并行，总耗时应明显小于串行的 0.6s
    assert elapsed < 0.5


def test_dependency_waits_for_upstream(orchestrator):
    publish(orchestrator, "chain", [("a", []), ("b", ["a"])])
    run, _ = orchestrator.trigger("chain", simulations={"a": {"duration_seconds": 0.1}})

    # a 完成前 b 不得启动；等待后两者均成功
    view = orchestrator.wait_terminal(run.run_id)
    assert [t.state for t in view.tasks]  # sanity
    assert view.state == RunState.SUCCEEDED
    a_finish = view.tasks[0].attempts[0].finished_at
    b_start = view.tasks[1].attempts[0].started_at
    assert b_start >= a_finish
