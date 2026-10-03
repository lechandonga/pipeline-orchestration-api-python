"""业务：任务级与运行级超时、超时重试与失败传播。"""

from __future__ import annotations

import pytest


@pytest.mark.business("任务超时-单次尝试失败并按重试规则继续")
def test_task_timeout_retries_then_fails(client, helpers):
    payload = {
        "name": "ttimeout",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "max_retries": 2,
                "timeout_seconds": 0.05,
                "action": {"type": "sleep", "seconds": 0.3},
            },
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "ttimeout")
    task = run["tasks"]["a"]
    assert run["state"] == "FAILED"
    assert task["state"] == "FAILED"
    assert len(task["attempts"]) == 3
    assert all(attempt["status"] == "FAILED" for attempt in task["attempts"])
    assert all("timed out" in (attempt["error"] or "") for attempt in task["attempts"])
    assert "timed out after 3 attempt(s)" in task["error"]


@pytest.mark.business("任务超时-慢任务超时但下次尝试成功")
def test_task_timeout_followed_by_success_attempt(client, helpers):
    payload = {
        "name": "tslow",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "max_retries": 2,
                "timeout_seconds": 0.1,
                "action": {
                    "type": "flaky",
                    "succeed_on_attempt": 2,
                    "seconds": 0.02,
                },
            },
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "tslow")
    task = run["tasks"]["a"]
    assert run["state"] == "SUCCEEDED"
    assert task["state"] == "SUCCEEDED"
    assert [a["status"] for a in task["attempts"]] == ["FAILED", "SUCCEEDED"]


@pytest.mark.business("任务超时-下游跳过且不记成功")
def test_task_timeout_propagates_skips(client, helpers):
    payload = {
        "name": "tprop",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "timeout_seconds": 0.05,
                "action": {"type": "sleep", "seconds": 0.3},
            },
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
            {"name": "side", "dependencies": [], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "tprop")
    tasks = run["tasks"]
    assert run["state"] == "FAILED"
    assert tasks["a"]["state"] == "FAILED"
    assert tasks["b"]["state"] == "SKIPPED"
    assert tasks["b"]["attempt_count"] == 0
    assert tasks["side"]["state"] == "SUCCEEDED"


@pytest.mark.business("运行超时-未开始不启动在途收敛原因明确")
def test_run_timeout_converges_with_clear_reason(client, helpers):
    payload = {
        "name": "rtimeout",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 2}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
            {"name": "c", "dependencies": [], "action": {"type": "sleep", "seconds": 2}},
        ],
    }
    helpers.submit(client, payload)
    response = client.post(
        "/pipelines/rtimeout/runs",
        json={"run_timeout_seconds": 0.1, "wait": True, "wait_timeout": 10},
    )
    assert response.status_code == 202, response.text
    run = response.json()
    assert run["state"] == "FAILED"
    assert run["failure_reason"] == "run exceeded timeout of 0.100s"
    assert run["tasks"]["b"]["state"] == "SKIPPED"
    assert run["tasks"]["b"]["attempt_count"] == 0
    # 等在途任务全部收敛后，绝不允许出现“运行失败但任务还在跑”
    settled = client.get(f"/runs/{run['run_id']}").json()
    for task in settled["tasks"].values():
        assert task["state"] in {"FAILED", "SKIPPED", "SUCCEEDED", "CANCELLED"}


@pytest.mark.business("兼容-不配超时行为保持旧逻辑")
def test_no_timeout_keeps_legacy_behavior(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    run = helpers.start_and_wait(client, "etl")
    assert run["state"] == "SUCCEEDED"
    assert run["timeout_seconds"] is None
    for task in run["tasks"].values():
        assert task["attempt_count"] == 1


@pytest.mark.business("超时校验-非法参数返回INVALID_ACTION")
def test_invalid_timeout_rejected(client, helpers):
    payload = {
        "name": "badtimeout",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "timeout_seconds": 0,
                "action": {"type": "succeed"},
            },
        ],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_ACTION"


@pytest.mark.business("超时校验-运行超时必须为正数")
def test_invalid_run_timeout_rejected(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    response = client.post(
        "/pipelines/etl/runs", json={"run_timeout_seconds": -1}
    )
    assert response.status_code == 422
