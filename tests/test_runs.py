"""业务：运行触发、并发、幂等与版本快照。"""

import threading

import pytest


@pytest.mark.business("运行调度-依赖顺序推进")
def test_tasks_run_in_dependency_order(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    run = helpers.start_and_wait(client, "etl")
    assert run["state"] == "SUCCEEDED"
    tasks = run["tasks"]
    assert [tasks[name]["state"] for name in ("extract", "transform", "load")] == [
        "SUCCEEDED",
        "SUCCEEDED",
        "SUCCEEDED",
    ]
    for name in tasks:
        assert tasks[name]["attempt_count"] == 1


@pytest.mark.business("幂等触发-重复只产生一个运行")
def test_idempotent_duplicate_returns_same_run(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    body = {"idempotency_key": "key-1", "wait": True, "wait_timeout": 15}
    first = client.post("/pipelines/etl/runs", json=body).json()
    second = client.post("/pipelines/etl/runs", json=body).json()
    assert first["created"] is True
    assert second["created"] is False
    assert first["run_id"] == second["run_id"]
    assert second["state"] == "SUCCEEDED"


@pytest.mark.business("幂等触发-并发去重")
def test_concurrent_idempotent_triggers_create_one_run(client, helpers, orchestrator):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    results: list = []
    barrier = threading.Barrier(8)

    def trigger():
        barrier.wait()
        run, created = orchestrator.start_run("etl", idempotency_key="race-key")
        results.append((run.run_id, created))

    threads = [threading.Thread(target=trigger) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({run_id for run_id, _ in results}) == 1
    assert sum(1 for _, created in results if created) == 1


@pytest.mark.business("并发运行-同流程多运行并存")
def test_multiple_concurrent_runs_isolated(client, helpers):
    payload = {
        "name": "slow",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": {"type": "sleep", "seconds": 0.3},
            },
            {
                "name": "b",
                "dependencies": ["a"],
                "action": {"type": "sleep", "seconds": 0.3},
            },
        ],
    }
    helpers.submit(client, payload)
    ids = [
        client.post("/pipelines/slow/runs", json={}).json()["run_id"] for _ in range(4)
    ]
    assert len(set(ids)) == 4
    for run_id in ids:
        run = client.post(f"/runs/{run_id}/wait?timeout=15", json={}).json()
        assert run["state"] == "SUCCEEDED"
        assert run["tasks"]["a"]["state"] == "SUCCEEDED"
        assert run["tasks"]["b"]["state"] == "SUCCEEDED"


@pytest.mark.business("版本兼容-旧运行按旧版本解释")
def test_old_run_keeps_old_version_after_new_submission(client, helpers):
    v1 = {
        "name": "evo",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": {"type": "fail", "message": "v1 boom"},
            },
        ],
    }
    helpers.submit(client, v1)
    run_v1 = helpers.start_and_wait(client, "evo")
    assert run_v1["version"] == 1
    assert run_v1["state"] == "FAILED"
    assert "v1 boom" in run_v1["tasks"]["a"]["error"]

    v2 = {
        "name": "evo",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, v2)
    # 历史运行结论不随后续版本变化
    old = client.get(f"/runs/{run_v1['run_id']}").json()
    assert old["state"] == "FAILED"
    assert set(old["tasks"]) == {"a"}
    # 新运行按新版本解释
    new_run = helpers.start_and_wait(client, "evo")
    assert new_run["version"] == 2
    assert new_run["state"] == "SUCCEEDED"
    # 可显式指定旧版本发起运行
    pinned = helpers.start_and_wait(client, "evo", version=1)
    assert pinned["version"] == 1
    assert pinned["state"] == "FAILED"


@pytest.mark.business("运行查询-不存在的资源")
def test_unknown_pipeline_and_run_404(client):
    assert client.post("/pipelines/nope/runs", json={}).status_code == 404
    assert client.get("/runs/does-not-exist").status_code == 404
