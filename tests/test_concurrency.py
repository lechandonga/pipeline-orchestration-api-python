"""业务：批量触发、全局并发容量、FIFO 排队与排队取消。"""

import os
import threading
import time

import pytest


@pytest.fixture
def capped_client():
    from fastapi.testclient import TestClient

    from app.api import create_app

    previous = os.environ.get("MAX_CONCURRENT_RUNS")
    os.environ["MAX_CONCURRENT_RUNS"] = "3"
    try:
        app = create_app()
        with TestClient(app) as test_client:
            yield test_client
    finally:
        if previous is None:
            os.environ.pop("MAX_CONCURRENT_RUNS", None)
        else:
            os.environ["MAX_CONCURRENT_RUNS"] = previous


@pytest.fixture
def fifo_orchestrator():
    from app.executor import LocalExecutor
    from app.orchestrator import Orchestrator
    from app.validation import DefinitionRegistry

    registry = DefinitionRegistry()
    orchestrator = Orchestrator(
        registry, LocalExecutor(), max_concurrent_runs=2, max_workers=16
    )
    try:
        yield registry, orchestrator
    finally:
        orchestrator.shutdown()


def submit_sleep_pipeline(client, name="batch", seconds=0.15):
    payload = {
        "name": name,
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": seconds}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 201, response.text


def trigger(client, name="batch"):
    return client.post(f"/pipelines/{name}/runs", json={}).json()["run_id"]


@pytest.mark.business("批量并发-默认配置下大量运行全部收敛")
def test_burst_of_runs_all_reach_terminal(client):
    submit_sleep_pipeline(client, name="burst", seconds=0.01)
    run_ids = [trigger(client, "burst") for _ in range(40)]
    deadline = time.time() + 30
    pending = set(run_ids)
    while pending and time.time() < deadline:
        pending = {
            rid
            for rid in pending
            if client.get(f"/runs/{rid}").json()["state"]
            not in ("SUCCEEDED", "FAILED", "CANCELLED")
        }
        if pending:
            time.sleep(0.05)
    assert not pending, f"runs stuck: {sorted(pending)[:5]}"
    for rid in run_ids:
        run = client.get(f"/runs/{rid}").json()
        assert run["state"] == "SUCCEEDED"
        assert run["tasks"]["a"]["state"] == "SUCCEEDED"
        assert run["tasks"]["b"]["state"] == "SUCCEEDED"


@pytest.mark.business("批量并发-压完后服务仍接新触发")
def test_service_accepts_new_runs_after_burst(client):
    submit_sleep_pipeline(client, name="recover", seconds=0.01)
    ids = [trigger(client, "recover") for _ in range(40)]
    for rid in ids:
        client.post(f"/runs/{rid}/wait?timeout=30", json={})
    after = client.post(
        "/pipelines/recover/runs", json={"wait": True, "wait_timeout": 10}
    )
    assert after.status_code == 202
    assert after.json()["state"] == "SUCCEEDED"


@pytest.mark.business("排队-超容量运行进入 QUEUED 且与 RUNNING 区分")
def test_overflow_runs_are_observably_queued(capped_client):
    submit_sleep_pipeline(capped_client, seconds=1.0)
    ids = [trigger(capped_client) for _ in range(8)]
    time.sleep(0.3)
    states = [capped_client.get(f"/runs/{rid}").json()["state"] for rid in ids]
    assert states.count("RUNNING") == 3
    assert states.count("QUEUED") == 5
    stats = capped_client.get("/runs/stats").json()
    assert stats["queued"] == 5
    assert stats["running_slots_active"] == 3
    assert stats["running_slots_capacity"] == 3
    for rid in ids:
        capped_client.post(f"/runs/{rid}/cancel")


@pytest.mark.business("排队-按触发先后放行不饿死")
def test_fifo_admission_order(fifo_orchestrator):
    registry, orchestrator = fifo_orchestrator
    registry.submit(
        {
            "name": "fifo",
            "tasks": [
                {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 5}},
            ],
        }
    )
    runs = [orchestrator.start_run("fifo")[0] for _ in range(6)]
    time.sleep(0.3)
    running = [
        r for r in runs if orchestrator.get_run(r.run_id).state.value == "RUNNING"
    ]
    queued = [
        r for r in runs if orchestrator.get_run(r.run_id).state.value == "QUEUED"
    ]
    assert [r.seq for r in running] == [1, 2]
    assert [r.seq for r in queued] == [3, 4, 5, 6]
    orchestrator.cancel_run(runs[0].run_id)
    deadline = time.time() + 5
    while time.time() < deadline:
        if orchestrator.get_run(runs[2].run_id).state.value == "RUNNING":
            break
        time.sleep(0.02)
    assert orchestrator.get_run(runs[2].run_id).state.value == "RUNNING"
    assert orchestrator.get_run(runs[3].run_id).state.value == "QUEUED"
    for run in runs[1:]:
        orchestrator.cancel_run(run.run_id)


@pytest.mark.business("排队取消-取消后一个任务都不启动")
def test_cancel_queued_run_starts_no_tasks(capped_client):
    submit_sleep_pipeline(capped_client, seconds=1.0)
    ids = [trigger(capped_client) for _ in range(5)]
    time.sleep(0.3)
    queued_id = ids[4]
    assert capped_client.get(f"/runs/{queued_id}").json()["state"] == "QUEUED"
    response = capped_client.post(f"/runs/{queued_id}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "CANCELLED"
    assert body["reason"] == "cancelled while queued"
    for task in body["tasks"].values():
        assert task["state"] == "SKIPPED"
        assert task["attempt_count"] == 0
    again = capped_client.post(f"/runs/{queued_id}/cancel")
    assert again.status_code == 409
    for rid in ids[:4]:
        capped_client.post(f"/runs/{rid}/cancel")
    time.sleep(0.2)
    final = capped_client.get(f"/runs/{queued_id}").json()
    assert final["state"] == "CANCELLED"
    assert all(t["attempt_count"] == 0 for t in final["tasks"].values())


@pytest.mark.business("排队场景-幂等触发仍只产生一个运行")
def test_idempotency_while_queued(capped_client):
    submit_sleep_pipeline(capped_client, seconds=1.0)
    ids = [trigger(capped_client) for _ in range(4)]
    barrier = threading.Barrier(6)
    results: list = []

    def replay():
        barrier.wait()
        response = capped_client.post(
            "/pipelines/batch/runs", json={"idempotency_key": "queue-key"}
        ).json()
        results.append(response["run_id"])

    threads = [threading.Thread(target=replay) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(set(results)) == 1
    queued_run = capped_client.get(f"/runs/{results[0]}").json()
    assert queued_run["state"] == "QUEUED"
    for rid in ids + results[:1]:
        capped_client.post(f"/runs/{rid}/cancel")
