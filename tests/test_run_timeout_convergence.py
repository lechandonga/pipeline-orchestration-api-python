"""业务：运行整体超时的终态收敛、读接口一致性、与取消的区分。

这些用例不依赖“刚好在某个瞬间撞上”的时机：超时收敛期间持续轮询，
断言每一次响应都满足不变量（运行 FAILED 时绝不能有任务 RUNNING）。
"""

from __future__ import annotations

import threading
import time

import pytest

_TERMINAL_TASK_STATES = {"SUCCEEDED", "FAILED", "SKIPPED", "CANCELLED"}


def _fan_out_payload(name: str = "fanout", *, task_timeout: float | None = None):
    # 5 个互相独立、在途时间足够长的任务 + 2 个永远轮不到启动的下游
    tasks = []
    for index in range(5):
        tasks.append(
            {
                "name": f"slow{index}",
                "dependencies": [],
                "max_retries": 3,
                "action": {"type": "sleep", "seconds": 2},
            }
        )
    tasks.append(
        {
            "name": "down1",
            "dependencies": ["slow0"],
            "action": {"type": "succeed"},
        }
    )
    tasks.append(
        {
            "name": "down2",
            "dependencies": ["slow1", "slow2"],
            "action": {"type": "succeed"},
        }
    )
    payload = {"name": name, "tasks": tasks}
    if task_timeout is not None:
        payload["default_task_timeout_seconds"] = task_timeout
    return payload


def _assert_self_consistent(record: dict) -> None:
    """核心不变量：终态运行不允许任何任务还在 PENDING/RUNNING。"""
    if record["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        for task in record["tasks"].values():
            assert task["state"] in _TERMINAL_TASK_STATES, (
                record["state"],
                task["name"],
                task["state"],
            )


@pytest.mark.business("运行超时-多个在途任务最终全部收敛且读接口一致")
def test_run_timeout_converges_multiple_inflight_tasks(client):
    client.post("/pipelines", json=_fan_out_payload())
    started = client.post(
        "/pipelines/fanout/runs",
        json={"run_timeout_seconds": 0.3},
    ).json()
    run_id = started["run_id"]

    # 收敛期间高频轮询详情：任何一次响应都不能自相矛盾
    deadline = time.time() + 10
    saw_running = False
    last = started
    while time.time() < deadline:
        last = client.get(f"/runs/{run_id}").json()
        _assert_self_consistent(last)
        if last["state"] == "RUNNING":
            saw_running = True
        if last["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(0.005)

    assert saw_running, "轮询应至少观察到一次 RUNNING 收敛过程"
    assert last["state"] == "FAILED"
    assert last["failure_reason"] == "run exceeded timeout of 0.300s"

    # 在途任务全部 FAILED，且超时后不允许再发起新的尝试（只跑过 1 次）
    for index in range(5):
        task = last["tasks"][f"slow{index}"]
        assert task["state"] == "FAILED", (index, task["state"])
        assert task["attempt_count"] == 1, (index, task["attempt_count"])
        assert task["attempts"][0]["status"] == "FAILED"
    # 未开始的下游一律 SKIPPED，零尝试
    for name in ("down1", "down2"):
        task = last["tasks"][name]
        assert task["state"] == "SKIPPED"
        assert task["attempt_count"] == 0
    # 没有任何任务被记成成功
    assert all(
        task["state"] != "SUCCEEDED" for task in last["tasks"].values()
    )

    # wait 接口返回与详情一致
    waited = client.post(f"/runs/{run_id}/wait", params={"timeout": 5}).json()
    _assert_self_consistent(waited)
    assert waited["state"] == "FAILED"
    assert waited["failure_reason"] == "run exceeded timeout of 0.300s"

    # 列表接口中的同一份记录也必须一致
    listed = client.get("/runs", params={"pipeline": "fanout"}).json()
    assert listed["total"] == 1
    row = listed["runs"][0]
    _assert_self_consistent(row)
    assert row["state"] == "FAILED"
    assert row["failure_reason"] == "run exceeded timeout of 0.300s"

    # 终态稳定：连续多次读取结果不再变化
    for _ in range(5):
        again = client.get(f"/runs/{run_id}").json()
        _assert_self_consistent(again)
        assert again["state"] == "FAILED"
        for name, task in again["tasks"].items():
            assert task["state"] == last["tasks"][name]["state"]
        time.sleep(0.01)


@pytest.mark.business("运行超时-并发轮询详情与列表全程不出现矛盾状态")
def test_concurrent_readers_never_see_contradiction(client):
    client.post("/pipelines", json=_fan_out_payload(name="fanrace"))
    run_id = client.post(
        "/pipelines/fanrace/runs",
        json={"run_timeout_seconds": 0.3},
    ).json()["run_id"]

    violations: list[str] = []
    stop = threading.Event()

    def poll_details() -> None:
        while not stop.is_set():
            record = client.get(f"/runs/{run_id}").json()
            try:
                _assert_self_consistent(record)
            except AssertionError:
                violations.append("detail:" + record["state"])

    def poll_list() -> None:
        while not stop.is_set():
            body = client.get("/runs", params={"pipeline": "fanrace"}).json()
            for row in body["runs"]:
                try:
                    _assert_self_consistent(row)
                except AssertionError:
                    violations.append("list:" + row["state"])

    threads = [
        threading.Thread(target=poll_details),
        threading.Thread(target=poll_details),
        threading.Thread(target=poll_list),
    ]
    for thread in threads:
        thread.start()

    final = client.post(
        f"/runs/{run_id}/wait", params={"timeout": 10}
    ).json()
    stop.set()
    for thread in threads:
        thread.join()

    assert final["state"] == "FAILED"
    assert violations == []


@pytest.mark.business("运行超时-与取消的终态和原因明确区分")
def test_timeout_vs_cancel_reasons_distinguishable(client):
    # 超时运行
    client.post(
        "/pipelines",
        json=_fan_out_payload(name="dist-reason"),
    )
    timed_out = client.post(
        "/pipelines/dist-reason/runs",
        json={"run_timeout_seconds": 0.2, "wait": True, "wait_timeout": 10},
    ).json()
    assert timed_out["state"] == "FAILED"
    assert "timeout" in timed_out["failure_reason"]

    # 取消运行
    started = client.post("/pipelines/dist-reason/runs", json={}).json()
    time.sleep(0.1)
    cancelled = client.post(
        f"/runs/{started['run_id']}/cancel"
    ).json()
    assert cancelled["state"] == "CANCELLED"
    assert "timeout" not in (cancelled["failure_reason"] or "")
    assert "cancel" in cancelled["failure_reason"]
    _assert_self_consistent(cancelled)

    # 列表里两者按状态可区分
    failed_rows = client.get(
        "/runs", params={"pipeline": "dist-reason", "state": "FAILED"}
    ).json()
    cancelled_rows = client.get(
        "/runs", params={"pipeline": "dist-reason", "state": "CANCELLED"}
    ).json()
    assert failed_rows["total"] == 1
    assert cancelled_rows["total"] == 1
    assert failed_rows["runs"][0]["failure_reason"] != cancelled_rows[
        "runs"
    ][0]["failure_reason"]


@pytest.mark.business("运行超时-超时后已在跑的任务绝不能记成功")
def test_inflight_task_never_succeeds_after_timeout(client, orchestrator):
    # 任务自身不超时、也不可被打断地“慢慢成功”：运行先超时，
    # 动作随后结束时也必须被收敛为 FAILED，不能记 SUCCEEDED。
    payload = {
        "name": "late-success",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "max_retries": 2,
                "action": {"type": "sleep", "seconds": 1.5},
            },
        ],
    }
    client.post("/pipelines", json=payload)
    run = client.post(
        "/pipelines/late-success/runs",
        json={"run_timeout_seconds": 0.2, "wait": True, "wait_timeout": 10},
    ).json()
    assert run["state"] == "FAILED"
    task = run["tasks"]["a"]
    assert task["state"] == "FAILED"
    assert task["attempt_count"] == 1
    assert task["attempts"][0]["status"] == "FAILED"


@pytest.mark.business("兼容-不配运行超时时在途任务正常成功")
def test_no_run_timeout_completes_normally(client):
    client.post("/pipelines", json=_fan_out_payload(name="notimeout-fan"))
    run = client.post(
        "/pipelines/notimeout-fan/runs",
        json={"wait": True, "wait_timeout": 15},
    ).json()
    assert run["state"] == "SUCCEEDED"
    assert run["timeout_seconds"] is None
    assert run["failure_reason"] is None
    for task in run["tasks"].values():
        assert task["state"] == "SUCCEEDED"
        assert task["attempt_count"] == 1
