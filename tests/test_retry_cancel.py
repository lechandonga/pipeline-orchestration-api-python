"""业务：取消、重试记录与失败传播。"""

import time

import pytest


@pytest.mark.business("重试-每次尝试都记录")
def test_flaky_task_retries_then_succeeds(client, helpers):
    payload = {
        "name": "flaky-pipe",
        "default_max_retries": 3,
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": {
                    "type": "flaky",
                    "succeed_on_attempt": 3,
                    "message": "transient",
                },
            },
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "flaky-pipe")
    assert run["state"] == "SUCCEEDED"
    task = run["tasks"]["a"]
    assert task["state"] == "SUCCEEDED"
    statuses = [attempt["status"] for attempt in task["attempts"]]
    assert statuses == ["FAILED", "FAILED", "SUCCEEDED"]
    assert task["attempts"][0]["error"] == "transient"


@pytest.mark.business("重试-上限后失败")
def test_retries_exhausted_marks_failed(client, helpers):
    payload = {
        "name": "always-fail",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "max_retries": 2,
                "action": {"type": "fail", "message": "boom"},
            },
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "always-fail")
    task = run["tasks"]["a"]
    assert run["state"] == "FAILED"
    assert task["state"] == "FAILED"
    assert len(task["attempts"]) == 3
    assert all(a["status"] == "FAILED" for a in task["attempts"])
    assert "boom" in task["error"]


@pytest.mark.business("失败传播-下游明确跳过")
def test_downstream_skipped_not_executed(client, helpers):
    payload = {
        "name": "propagate",
        "tasks": [
            {
                "name": "root",
                "dependencies": [],
                "action": {"type": "fail", "message": "root boom"},
            },
            {"name": "mid", "dependencies": ["root"], "action": {"type": "succeed"}},
            {"name": "leaf", "dependencies": ["mid"], "action": {"type": "succeed"}},
            {"name": "independent", "dependencies": [], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "propagate")
    tasks = run["tasks"]
    assert tasks["root"]["state"] == "FAILED"
    assert tasks["mid"]["state"] == "SKIPPED"
    assert tasks["leaf"]["state"] == "SKIPPED"
    # 跳过的任务从未执行，也不计入成功
    assert tasks["mid"]["attempt_count"] == 0
    assert tasks["leaf"]["attempt_count"] == 0
    assert "upstream" in tasks["leaf"]["error"]
    # 无依赖关系的分支不受影响
    assert tasks["independent"]["state"] == "SUCCEEDED"
    assert run["state"] == "FAILED"


@pytest.mark.business("重复记账-成功任务不重复执行")
def test_succeeded_task_not_executed_again(client, helpers, orchestrator):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    first = helpers.start_and_wait(client, "etl", idempotency_key="do-once")
    repeat = helpers.start_and_wait(client, "etl", idempotency_key="do-once")
    assert repeat["run_id"] == first["run_id"]
    for name, task in repeat["tasks"].items():
        assert task["attempt_count"] == 1, (name, task["attempt_count"])


@pytest.mark.business("取消-未开始任务不再启动")
def test_cancel_skips_unstarted_tasks(client, helpers):
    payload = {
        "name": "cancel-me",
        "tasks": [
            {
                "name": "slow",
                "dependencies": [],
                "action": {"type": "sleep", "seconds": 5},
            },
            {"name": "after", "dependencies": ["slow"], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    started = client.post("/pipelines/cancel-me/runs", json={}).json()
    run_id = started["run_id"]
    # 等待 slow 进入 RUNNING
    deadline = time.time() + 5
    while time.time() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        if run["tasks"]["slow"]["state"] == "RUNNING":
            break
        time.sleep(0.02)
    cancelled = client.post(f"/runs/{run_id}/cancel").json()
    assert cancelled["state"] == "CANCELLED"
    assert cancelled["tasks"]["after"]["state"] == "SKIPPED"
    assert cancelled["tasks"]["after"]["attempt_count"] == 0
    # 进行中的任务收敛到明确终态（被中断 -> CANCELLED）
    assert cancelled["tasks"]["slow"]["state"] == "CANCELLED"


@pytest.mark.business("取消-终态运行不可再取消")
def test_cancel_terminal_run_rejected(client, helpers):
    helpers.submit(client, helpers.SIMPLE_PIPELINE)
    run = helpers.start_and_wait(client, "etl")
    response = client.post(f"/runs/{run['run_id']}/cancel")
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "RUN_NOT_CANCELLABLE"


@pytest.mark.business("取消-快速完成后状态一致")
def test_cancel_race_converges_consistent(client, helpers):
    payload = {
        "name": "quick",
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": {"type": "sleep", "seconds": 0.05},
            },
            {
                "name": "b",
                "dependencies": ["a"],
                "action": {"type": "sleep", "seconds": 0.05},
            },
        ],
    }
    helpers.submit(client, payload)
    started = client.post("/pipelines/quick/runs", json={}).json()
    response = client.post(f"/runs/{started['run_id']}/cancel")
    assert response.status_code in (200, 409)
    final = client.get(f"/runs/{started['run_id']}").json()
    assert final["state"] in {"CANCELLED", "SUCCEEDED"}
    for task in final["tasks"].values():
        assert task["state"] not in {"PENDING", "RUNNING"}
