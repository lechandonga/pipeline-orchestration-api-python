"""业务：定义重复提交的版本复用——跨版本回退、名字空白、并发提交。"""

from __future__ import annotations

import threading

import pytest


def _v1():
    return {
        "name": "rollback-pipe",
        "default_max_retries": 1,
        "default_task_timeout_seconds": 2.5,
        "tasks": [
            {
                "name": "a",
                "dependencies": [],
                "action": {"type": "fail", "message": "v1"},
                "max_retries": 2,
                "timeout_seconds": 1.0,
            },
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }


def _v2():
    payload = _v1()
    payload["tasks"] = [
        {
            "name": "a",
            "dependencies": [],
            "action": {"type": "succeed"},
            "max_retries": 2,
            "timeout_seconds": 1.0,
        },
        {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        {"name": "c", "dependencies": ["b"], "action": {"type": "succeed"}},
    ]
    return payload


@pytest.mark.business("版本复用-回退重发任意历史版本复用最早版本号")
def test_rollback_to_any_historical_version_reuses(client):
    first = client.post("/pipelines", json=_v1()).json()
    second = client.post("/pipelines", json=_v2()).json()
    assert first["version"] == 1 and first["reused"] is False
    assert second["version"] == 2 and second["reused"] is False

    # 原样重新提交 v1 的内容：必须复用历史上的 v1，而不是生成 v3
    rolled_back = client.post("/pipelines", json=_v1()).json()
    assert rolled_back["version"] == 1
    assert rolled_back["reused"] is True

    versions = client.get("/pipelines/rollback-pipe/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2]
    # 再发一遍 v2，同样复用
    again_v2 = client.post("/pipelines", json=_v2()).json()
    assert again_v2["version"] == 2
    assert again_v2["reused"] is True
    versions = client.get("/pipelines/rollback-pipe/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2]


@pytest.mark.business("版本复用-名字前后空白不影响复用")
def test_surrounding_whitespace_name_reuses(client):
    client.post("/pipelines", json=_v1()).json()
    client.post("/pipelines", json=_v2()).json()
    payload = _v1()
    payload["name"] = "  rollback-pipe  "
    result = client.post("/pipelines", json=payload).json()
    assert result["version"] == 1
    assert result["reused"] is True
    versions = client.get(
        "/pipelines/rollback-pipe/versions"
    ).json()["versions"]
    assert len(versions) == 2


@pytest.mark.business("版本复用-仅重试或超时配置变化也算新版本")
def test_config_only_change_creates_version(client):
    payload = _v1()
    payload["name"] = "cfg-pipe"
    client.post("/pipelines", json=payload)
    changed = dict(payload)
    changed["default_max_retries"] = 0
    result = client.post("/pipelines", json=changed).json()
    assert result["version"] == 2
    assert result["reused"] is False
    # 改回来仍然复用 v1
    back = client.post("/pipelines", json=payload).json()
    assert back["version"] == 1
    assert back["reused"] is True


@pytest.mark.business("版本复用-并发提交同一份定义只产生一个版本")
def test_concurrent_identical_submissions_share_one_version():
    from app.validation import DefinitionRegistry

    registry = DefinitionRegistry()
    # 先制造两个不同版本，模拟回退并发重发 v1 的场景
    registry.submit(_v1())
    registry.submit(_v2())

    results: list[tuple[int, bool]] = []
    barrier = threading.Barrier(8)
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        result = registry.submit(_v1())
        with lock:
            results.append((result.version, result.reused))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [(1, True)] * 8
    versions = registry.list_versions("rollback-pipe")
    assert [v.version for v in versions] == [1, 2]


@pytest.mark.business("版本复用-并发首发全新定义也只产生一个版本")
def test_concurrent_first_submissions_share_one_version():
    from app.validation import DefinitionRegistry

    registry = DefinitionRegistry()
    payload = {
        "name": "brand-new",
        "tasks": [{"name": "a", "dependencies": []}],
    }
    outcomes: list[tuple[int, bool]] = []
    barrier = threading.Barrier(6)
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        result = registry.submit(payload)
        with lock:
            outcomes.append((result.version, result.reused))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    created = [outcome for outcome in outcomes if outcome == (1, False)]
    reused = [outcome for outcome in outcomes if outcome == (1, True)]
    assert len(created) == 1
    assert len(reused) == 5
    assert len(registry.list_versions("brand-new")) == 1
