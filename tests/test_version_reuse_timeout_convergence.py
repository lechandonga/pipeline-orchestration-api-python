"""业务：跨版本/回退/并发的定义复用，以及运行超时的终态收敛一致性。"""

from __future__ import annotations

import threading
import time

import pytest

from app.validation import DefinitionRegistry


def _pipe(name, task_action=None, default_retries=0, default_timeout=None):
    payload = {
        "name": name,
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": task_action or {"type": "succeed"},
            },
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    if default_retries:
        payload["default_max_retries"] = default_retries
    if default_timeout is not None:
        payload["default_task_timeout_seconds"] = default_timeout
    return payload


@pytest.mark.business("定义复用-回退重发任意历史版本")
def test_rollback_resubmit_reuses_oldest_version(client, helpers):
    v1 = _pipe("rollback", {"type": "fail", "message": "v1"})
    v2 = _pipe("rollback", {"type": "succeed"})
    v3 = _pipe("rollback", {"type": "flaky", "succeed_on_attempt": 2})
    r1 = helpers.submit(client, v1)
    r2 = helpers.submit(client, v2)
    r3 = helpers.submit(client, v3)
    assert [r1["version"], r2["version"], r3["version"]] == [1, 2, 3]

    # 原样重发最早的 v1（名字前后多敲空格），必须复用版本 1 而不是生出 v4
    rollback = dict(v1)
    rollback["name"] = "  rollback  "
    reused = helpers.submit(client, rollback)
    assert reused["version"] == 1
    assert reused["reused"] is True

    versions = client.get("/pipelines/rollback/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2, 3]
    # 版本列表里不允许出现内容重复、版本号不同的记录
    fingerprints = {
        (
            v["default_max_retries"],
            v["default_task_timeout_seconds"],
            tuple((t["name"], tuple(sorted(t["dependencies"])),
                   t["action"]["type"], t["max_retries"], t["timeout_seconds"])
                  for t in v["tasks"]),
        )
        for v in versions
    }
    assert len(fingerprints) == 3

    # 再发中间版本 v2 同样复用，版本号依旧连续唯一
    reused_v2 = helpers.submit(client, v2)
    assert reused_v2["version"] == 2
    assert reused_v2["reused"] is True
    versions = client.get("/pipelines/rollback/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2, 3]


@pytest.mark.business("定义复用-重试与超时配置参与比对")
def test_retry_and_timeout_config_participate_in_reuse(client, helpers):
    base = _pipe("cfg", default_retries=1, default_timeout=2.0)
    helpers.submit(client, base)
    # 只有默认重试数不同 -> 新版本
    changed_retries = _pipe("cfg", default_retries=2, default_timeout=2.0)
    r = helpers.submit(client, changed_retries)
    assert r["version"] == 2 and r["reused"] is False
    # 回到完全相同配置 -> 复用版本 1（跨版本，不只是最近一版）
    back = helpers.submit(client, base)
    assert back["version"] == 1 and back["reused"] is True
    # 默认任务超时不同 -> 新版本
    changed_timeout = _pipe("cfg", default_retries=2, default_timeout=3.0)
    r = helpers.submit(client, changed_timeout)
    assert r["version"] == 3 and r["reused"] is False
    back_again = helpers.submit(client, base)
    assert back_again["version"] == 1 and back_again["reused"] is True


@pytest.mark.business("定义复用-并发重复提交只得一个版本")
def test_concurrent_identical_submissions_share_one_version():
    registry = DefinitionRegistry()
    # 先造两个历史版本
    registry.submit(_pipe("race", {"type": "fail", "message": "v1"}))
    registry.submit(_pipe("race", {"type": "succeed"}))
    payload = _pipe("race", {"type": "fail", "message": "v1"})

    results: list = []
    barrier = threading.Barrier(24)

    def worker():
        barrier.wait()
        results.append(registry.submit(payload))

    threads = [threading.Thread(target=worker) for _ in range(24)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len({r.version for r in results}) == 1
    assert results[0].version == 1
    assert all(r.reused for r in results)
    versions = registry.list_versions("race")
    assert [v.version for v in versions] == [1, 2]


def _assert_no_contradiction(run):
    if run["state"] == "RUNNING":
        return
    assert run["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}
    for task in run["tasks"].values():
        assert task["state"] != "RUNNING", (
            f"run {run['state']} but task {task['name']} still RUNNING"
        )
        assert task["state"] != "PENDING", (
            f"run {run['state']} but task {task['name']} still PENDING"
        )


@pytest.mark.business("运行超时-多个在途任务收敛且各读接口一致")
def test_run_timeout_multiple_inflight_read_consistency(client, helpers):
    payload = {
        "name": "many-inflight",
        "tasks": [
            {"name": "a", "dependencies": [],
             "action": {"type": "sleep", "seconds": 5}},
            {"name": "b", "dependencies": [],
             "action": {"type": "sleep", "seconds": 5}},
            {"name": "c", "dependencies": [],
             "action": {"type": "sleep", "seconds": 5}},
            {"name": "down", "dependencies": ["a", "b", "c"],
             "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    started = client.post(
        "/pipelines/many-inflight/runs",
        json={"run_timeout_seconds": 0.2},
    ).json()
    run_id = started["run_id"]

    # 确认到点时确实有任务在跑（不撞时机：轮询直到观察到 RUNNING 任务）
    observed_running_task = False
    deadline = time.time() + 5
    while time.time() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        states = {t["state"] for t in run["tasks"].values()}
        if "RUNNING" in states:
            observed_running_task = True
        if run["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(0.005)
    assert observed_running_task, "超时触发时应当存在在途任务"

    # 多线程同时轮询 详情/列表/统计，任何时刻都不许出现自相矛盾的记录
    stop = threading.Event()
    contradictions: list[str] = []

    def poll_detail():
        while not stop.is_set():
            _safe_check(contradictions,
                        lambda: _assert_no_contradiction(
                            client.get(f"/runs/{run_id}").json()))

    def poll_list():
        while not stop.is_set():
            data = client.get(
                "/runs?pipeline=many-inflight&limit=10"
            ).json()
            for run in data["runs"]:
                _safe_check(contradictions,
                            lambda r=run: _assert_no_contradiction(r))

    def poll_stats():
        while not stop.is_set():
            client.get("/runs-stats?pipeline=many-inflight").json()

    pollsters = [threading.Thread(target=poll_detail),
                 threading.Thread(target=poll_list),
                 threading.Thread(target=poll_stats)]
    for t in pollsters:
        t.start()

    final = client.post(
        f"/runs/{run_id}/wait", params={"timeout": 15}
    ).json()
    stop.set()
    for t in pollsters:
        t.join()

    assert not contradictions, contradictions
    assert final["state"] == "FAILED"
    assert final["failure_reason"] == "run exceeded timeout of 0.200s"
    for name in ("a", "b", "c"):
        task = final["tasks"][name]
        assert task["state"] == "FAILED", (name, task["state"])
        # 在途任务被整体超时中断：绝不能记成成功，也不许补发重试
        assert all(a["status"] == "FAILED" for a in task["attempts"])
        assert task["attempt_count"] == 1
    assert final["tasks"]["down"]["state"] == "SKIPPED"
    assert final["tasks"]["down"]["attempt_count"] == 0

    detail = client.get(f"/runs/{run_id}").json()
    listed = [r for r in client.get(
        "/runs?pipeline=many-inflight").json()["runs"]
              if r["run_id"] == run_id][0]
    assert detail["state"] == listed["state"] == "FAILED"
    assert detail["failure_reason"] == listed["failure_reason"]


def _safe_check(contradictions, check):
    try:
        check()
    except AssertionError as exc:
        contradictions.append(str(exc))


@pytest.mark.business("运行超时-不发起新尝试且下游不解锁")
def test_run_timeout_interrupts_without_retry_or_unlock(client, helpers):
    payload = {
        "name": "no-retry",
        "tasks": [
            {"name": "a", "dependencies": [], "max_retries": 3,
             "action": {"type": "sleep", "seconds": 5}},
            {"name": "b", "dependencies": ["a"],
             "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    run = client.post(
        "/pipelines/no-retry/runs",
        json={"run_timeout_seconds": 0.2, "wait": True, "wait_timeout": 15},
    ).json()
    assert run["state"] == "FAILED"
    assert "exceeded timeout" in run["failure_reason"]
    assert run["tasks"]["a"]["state"] == "FAILED"
    assert run["tasks"]["a"]["attempt_count"] == 1
    assert run["tasks"]["a"]["attempts"][0]["status"] == "FAILED"
    assert run["tasks"]["b"]["state"] == "SKIPPED"
    assert run["tasks"]["b"]["attempt_count"] == 0


@pytest.mark.business("取消与超时-终态和原因可区分")
def test_cancel_and_timeout_outcomes_are_distinguishable(client, helpers):
    slow = {
        "name": "dist",
        "tasks": [
            {"name": "a", "dependencies": [],
             "action": {"type": "sleep", "seconds": 5}},
        ],
    }
    helpers.submit(client, slow)

    cancelled_start = client.post("/pipelines/dist/runs", json={}).json()
    run_id = cancelled_start["run_id"]
    _wait_task_running(client, run_id, "a")
    cancelled = client.post(f"/runs/{run_id}/cancel").json()
    assert cancelled["state"] == "CANCELLED"
    assert "cancel" in cancelled["failure_reason"]
    assert "timeout" not in cancelled["failure_reason"]
    assert cancelled["tasks"]["a"]["state"] == "CANCELLED"

    timed_out = client.post(
        "/pipelines/dist/runs",
        json={"run_timeout_seconds": 0.2, "wait": True, "wait_timeout": 15},
    ).json()
    assert timed_out["state"] == "FAILED"
    assert "timeout" in timed_out["failure_reason"]
    assert timed_out["tasks"]["a"]["state"] == "FAILED"


@pytest.mark.business("排队取消-一个任务都不会启动")
def test_queued_cancel_starts_nothing(client, helpers, orchestrator):
    # 占满容量，让新运行稳定停在 QUEUED
    payload = {
        "name": "blocker",
        "tasks": [
            {"name": "a", "dependencies": [],
             "action": {"type": "sleep", "seconds": 5}},
        ],
    }
    helpers.submit(client, payload)
    capacity = orchestrator.max_concurrent_runs
    blockers = [
        client.post("/pipelines/blocker/runs", json={}).json()["run_id"]
        for _ in range(capacity)
    ]
    for rid in blockers:
        _wait_state(client, rid, "RUNNING")

    queued = client.post("/pipelines/blocker/runs", json={}).json()
    assert queued["state"] == "QUEUED"
    cancelled = client.post(
        f"/runs/{queued['run_id']}/cancel"
    ).json()
    assert cancelled["state"] == "CANCELLED"
    assert "cancelled while queued" in cancelled["failure_reason"]
    for task in cancelled["tasks"].values():
        assert task["state"] == "SKIPPED"
        assert task["attempt_count"] == 0

    # 再查一次，结论稳定；等待接口同样立即给出终态
    again = client.post(
        f"/runs/{queued['run_id']}/wait", params={"timeout": 5}
    ).json()
    assert again["state"] == "CANCELLED"
    assert all(t["attempt_count"] == 0 for t in again["tasks"].values())


@pytest.mark.business("兼容-无整体超时运行不受收敛逻辑影响")
def test_no_run_timeout_success_path_unchanged(client, helpers):
    payload = {
        "name": "plain",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    helpers.submit(client, payload)
    run = helpers.start_and_wait(client, "plain")
    assert run["state"] == "SUCCEEDED"
    assert run["failure_reason"] is None
    assert run["timeout_seconds"] is None
    assert {n: t["state"] for n, t in run["tasks"].items()} == {
        "a": "SUCCEEDED", "b": "SUCCEEDED"
    }


def _wait_task_running(client, run_id, task_name, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        if run["tasks"][task_name]["state"] == "RUNNING":
            return
        time.sleep(0.01)
    raise AssertionError(f"task {task_name} never reached RUNNING")


def _wait_state(client, run_id, state, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        if run["state"] == state:
            return
        time.sleep(0.01)
    raise AssertionError(f"run {run_id} never reached {state}")
