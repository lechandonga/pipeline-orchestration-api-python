"""业务：任务级超时与运行级超时。"""

import time

import pytest


def submit(client, payload):
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.business("任务超时-本次尝试失败并按重试规则继续")
def test_task_timeout_retries_then_succeeds(client):
    submit(
        client,
        {
            "name": "tt",
            "tasks": [
                {
                    "name": "slow",
                    "dependencies": [],
                    "max_retries": 1,
                    "timeout_seconds": 0.2,
                    "action": {"type": "sleep", "seconds": 5},
                },
            ],
        },
    )
    started = time.time()
    response = client.post(
        "/pipelines/tt/runs",
        json={"wait": True, "wait_timeout": 15},
    )
    elapsed = time.time() - started
    run = response.json()
    assert run["state"] == "FAILED"
    task = run["tasks"]["slow"]
    assert task["state"] == "FAILED"
    assert [a["status"] for a in task["attempts"]] == ["FAILED", "FAILED"]
    assert all("timed out" in a["error"] for a in task["attempts"])
    assert "timed out" in task["error"]
    assert elapsed < 3, "timeout should abort attempts promptly"


@pytest.mark.business("任务超时-重试用尽走失败与下游跳过链路")
def test_task_timeout_propagates_skips_downstream(client):
    submit(
        client,
        {
            "name": "tt-prop",
            "tasks": [
                {
                    "name": "slow",
                    "dependencies": [],
                    "max_retries": 0,
                    "timeout_seconds": 0.2,
                    "action": {"type": "sleep", "seconds": 5},
                },
                {"name": "next", "dependencies": ["slow"], "action": {"type": "succeed"}},
            ],
        },
    )
    run = client.post(
        "/pipelines/tt-prop/runs", json={"wait": True, "wait_timeout": 10}
    ).json()
    assert run["state"] == "FAILED"
    assert run["tasks"]["slow"]["state"] == "FAILED"
    assert run["tasks"]["slow"]["attempt_count"] == 1
    assert run["tasks"]["next"]["state"] == "SKIPPED"
    assert run["tasks"]["next"]["attempt_count"] == 0


@pytest.mark.business("任务超时-绝不记成成功")
def test_task_that_finishes_after_timeout_is_not_success(client):
    # The executor action completes well after the attempt deadline fires.
    submit(
        client,
        {
            "name": "tt-late",
            "tasks": [
                {
                    "name": "slow",
                    "dependencies": [],
                    "max_retries": 0,
                    "timeout_seconds": 0.15,
                    "action": {"type": "sleep", "seconds": 0.6},
                },
            ],
        },
    )
    run = client.post(
        "/pipelines/tt-late/runs", json={"wait": True, "wait_timeout": 10}
    ).json()
    task = run["tasks"]["slow"]
    assert task["state"] == "FAILED"
    assert task["attempts"][0]["status"] == "FAILED"
    assert "timed out" in task["attempts"][0]["error"]


@pytest.mark.business("任务超时-未超时的任务正常成功且只记一次")
def test_timeout_budget_does_not_fire_for_fast_task(client):
    submit(
        client,
        {
            "name": "tt-ok",
            "tasks": [
                {
                    "name": "quick",
                    "dependencies": [],
                    "max_retries": 1,
                    "timeout_seconds": 2,
                    "action": {"type": "flaky", "succeed_on_attempt": 2, "seconds": 0},
                },
            ],
        },
    )
    run = client.post(
        "/pipelines/tt-ok/runs", json={"wait": True, "wait_timeout": 10}
    ).json()
    assert run["state"] == "SUCCEEDED"
    task = run["tasks"]["quick"]
    assert [a["status"] for a in task["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert "timed out" not in (task["attempts"][0]["error"] or "")


@pytest.mark.business("运行超时-未开始任务不启动且在途收敛")
def test_run_timeout_stops_everything(client):
    submit(
        client,
        {
            "name": "rt",
            "tasks": [
                {
                    "name": "slow",
                    "dependencies": [],
                    "action": {"type": "sleep", "seconds": 5},
                },
                {"name": "after", "dependencies": ["slow"], "action": {"type": "succeed"}},
            ],
        },
    )
    timed = client.post(
        "/pipelines/rt/runs", json={"timeout_seconds": 0.3, "wait": True, "wait_timeout": 10}
    ).json()
    assert timed["state"] == "FAILED"
    assert timed["reason"] == "run timed out after 0.3s"
    assert timed["tasks"]["after"]["state"] == "SKIPPED"
    assert timed["tasks"]["after"]["attempt_count"] == 0
    assert timed["tasks"]["slow"]["state"] in ("CANCELLED", "FAILED")


@pytest.mark.business("运行超时-参数非法被拒")
def test_run_timeout_must_be_positive(client):
    submit(client, {"name": "rt-bad", "tasks": [{"name": "a", "dependencies": []}]})
    response = client.post("/pipelines/rt-bad/runs", json={"timeout_seconds": 0})
    assert response.status_code == 422


@pytest.mark.business("超时兼容-未配置超时行为与旧版一致")
def test_no_timeout_keeps_legacy_behavior(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    run = helpers.start_and_wait(client, "etl")
    assert run["state"] == "SUCCEEDED"
    assert run["timeout_seconds"] is None
    assert all(
        task_detail["attempts"][0]["status"] == "SUCCEEDED"
        for task_detail in run["tasks"].values()
    )
    definition = client.get("/pipelines/etl").json()
    assert definition["tasks"][0]["timeout_seconds"] is None
