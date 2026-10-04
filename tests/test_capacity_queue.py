"""业务：批量触发、容量排队、排队取消与 FIFO 公平性。"""

from __future__ import annotations

import time

import pytest


@pytest.mark.business("批量并发-三四十个运行全部收敛")
def test_burst_of_runs_all_reach_terminal(client, helpers):
    payload = {
        "name": "short",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    burst = 40
    run_ids = [
        client.post("/pipelines/short/runs", json={}).json()["run_id"]
        for _ in range(burst)
    ]
    deadline = time.time() + 30
    finals = {}
    while time.time() < deadline and len(finals) < burst:
        for run_id in run_ids:
            state = client.get(f"/runs/{run_id}").json()["state"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                finals[run_id] = state
        time.sleep(0.05)
    assert len(finals) == burst
    assert all(state == "SUCCEEDED" for state in finals.values())


@pytest.mark.business("容量排队-超过上限进入可见QUEUED")
def test_runs_beyond_capacity_are_queued(client, helpers, orchestrator):
    assert orchestrator.max_concurrent_runs == 8
    payload = {
        "name": "hold",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 1}},
        ],
    }
    helpers.submit(client, payload)
    ids = [
        client.post("/pipelines/hold/runs", json={}).json()["run_id"]
        for _ in range(12)
    ]
    time.sleep(0.3)
    running, queued = [], []
    for run_id in ids:
        state = client.get(f"/runs/{run_id}").json()["state"]
        if state == "RUNNING":
            running.append(run_id)
        elif state == "QUEUED":
            queued.append(run_id)
    assert len(running) == 8
    assert len(queued) == 4
    stats = client.get("/runs-stats?pipeline=hold").json()
    assert stats["queued"] == 4
    assert stats["running"] == 8


@pytest.mark.business("容量排队-FIFO顺序执行不饿死")
def test_queue_is_fifo_without_starvation(client, helpers, orchestrator):
    payload = {
        "name": "fifo",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 0.5}},
        ],
    }
    helpers.submit(client, payload)
    # 容量限制为 1：每次只有一个运行在执行，后续运行严格排队，准入顺序
    # 必然等于触发顺序（FIFO），不依赖完成先后的观察时机。
    orchestrator.max_concurrent_runs = 1
    ids = [
        client.post("/pipelines/fifo/runs", json={}).json()["run_id"]
        for _ in range(5)
    ]
    order: list[str] = []
    done: set[str] = set()
    deadline = time.time() + 15
    admitted: list[str] = []
    seen_running: set[str] = set()
    while time.time() < deadline and len(done) < 5:
        for run_id in ids:
            run = client.get(f"/runs/{run_id}").json()
            if (
                run["state"] == "RUNNING"
                and run_id not in seen_running
            ):
                seen_running.add(run_id)
                admitted.append(run_id)
            if run_id in done:
                continue
            if run["state"] == "SUCCEEDED":
                order.append(run_id)
                done.add(run_id)
        time.sleep(0.02)
    assert len(order) == 5
    assert admitted == ids  # 准入顺序即 FIFO
    assert order == ids  # 容量 1 下完成顺序同样等于准入顺序


@pytest.mark.business("排队取消-一个任务都不启动")
def test_cancel_queued_run_starts_no_tasks(client, helpers):
    payload = {
        "name": "qcancel",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 1}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    ids = [
        client.post("/pipelines/qcancel/runs", json={}).json()["run_id"]
        for _ in range(10)
    ]
    time.sleep(0.3)
    victim = next(
        rid for rid in ids if client.get(f"/runs/{rid}").json()["state"] == "QUEUED"
    )
    cancelled = client.post(f"/runs/{victim}/cancel").json()
    assert cancelled["state"] == "CANCELLED"
    assert cancelled["failure_reason"] == "cancelled while queued: no task was started"
    for task in cancelled["tasks"].values():
        assert task["state"] == "SKIPPED"
        assert task["attempt_count"] == 0
        assert task["started_at"] is None
    for run_id in ids:
        if run_id == victim:
            continue
        final = client.post(f"/runs/{run_id}/wait?timeout=15", json={}).json()
        assert final["state"] == "SUCCEEDED"
    victim_again = client.get(f"/runs/{victim}").json()
    assert victim_again["state"] == "CANCELLED"
    assert all(t["attempt_count"] == 0 for t in victim_again["tasks"].values())
    assert client.post(f"/runs/{victim}/cancel").status_code == 409


@pytest.mark.business("批量并发-压完后仍可接新触发")
def test_service_accepts_new_triggers_after_burst(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    for _ in range(36):
        client.post("/pipelines/etl/runs", json={})
    time.sleep(2)
    after = helpers.start_and_wait(client, "etl", idempotency_key="after-burst")
    assert after["state"] == "SUCCEEDED"
    assert after["created"] is True


@pytest.mark.business("排队幂等-QUEUED期间重复触发不新建运行")
def test_idempotent_replay_while_queued(client, helpers):
    payload = {
        "name": "idemq",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 1}},
        ],
    }
    helpers.submit(client, payload)
    for _ in range(10):
        client.post("/pipelines/idemq/runs", json={})
    body = {"idempotency_key": "same-key"}
    first = client.post("/pipelines/idemq/runs", json=body).json()
    assert first["state"] == "QUEUED"
    second = client.post("/pipelines/idemq/runs", json=body).json()
    assert second["run_id"] == first["run_id"]
    assert second["created"] is False
    client.post(f"/runs/{first['run_id']}/cancel")
    third = client.post("/pipelines/idemq/runs", json=body).json()
    assert third["run_id"] == first["run_id"]
    assert third["state"] == "CANCELLED"
    assert third["created"] is False
