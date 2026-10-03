"""业务：提交边界——名称空白归一、版本连续唯一、相同定义复用版本。"""

from __future__ import annotations

import threading

import pytest


@pytest.mark.business("提交边界-名称前后空格算同一流程")
def test_whitespace_padded_name_is_same_pipeline(client, helpers):
    payload = {
        "name": "  etl-x  ",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    first = helpers.submit(client, payload)
    assert first["name"] == "etl-x"
    # 用带前后空格的名字查定义、发起运行、查版本都指向同一个流程
    versions = client.get("/pipelines/%20%20etl-x%20/versions").json()
    assert versions["name"] == "etl-x"
    run = helpers.start_and_wait(client, " etl-x ")
    assert run["pipeline"] == "etl-x"
    assert run["state"] == "SUCCEEDED"


@pytest.mark.business("提交边界-版本号连续唯一")
def test_versions_are_sequential_and_unique(client, helpers):
    payload = {
        "name": "ver",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    v1 = helpers.submit(client, payload)
    assert v1["version"] == 1 and v1["reused"] is False
    payload["tasks"].append(
        {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}}
    )
    v2 = helpers.submit(client, payload)
    assert v2["version"] == 2 and v2["reused"] is False
    payload["tasks"].append(
        {"name": "c", "dependencies": ["b"], "action": {"type": "fail"}}
    )
    v3 = helpers.submit(client, payload)
    assert v3["version"] == 3 and v3["reused"] is False
    versions = client.get("/pipelines/ver/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2, 3]


@pytest.mark.business("提交边界-重复相同定义复用版本")
def test_repeated_identical_submission_reuses_version(client, helpers):
    payload = {
        "name": "same",
        "default_max_retries": 1,
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }
    first = helpers.submit(client, payload)
    again = helpers.submit(client, {**payload, "name": "  same  "})
    assert again["version"] == first["version"] == 1
    assert again["reused"] is True


@pytest.mark.business("提交边界-超时配置变化产生新版本")
def test_timeout_change_creates_new_version(client, helpers):
    payload = {
        "name": "tout-ver",
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
        ],
    }
    v1 = helpers.submit(client, payload)
    payload["tasks"][0]["timeout_seconds"] = 5
    v2 = helpers.submit(client, payload)
    assert v2["version"] == v1["version"] + 1 and v2["reused"] is False
    payload["tasks"][0].pop("timeout_seconds")
    payload["default_task_timeout_seconds"] = 3
    v3 = helpers.submit(client, payload)
    assert v3["version"] == 3 and v3["reused"] is False


@pytest.mark.business("提交边界-并发同名提交不产生重复版本")
def test_concurrent_submissions_share_version(client, helpers, orchestrator):
    payload = {
        "name": "race-submit",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    versions: list[int] = []
    barrier = threading.Barrier(6)

    def submit():
        barrier.wait()
        result = orchestrator.registry.submit(payload)
        versions.append(result.version)

    threads = [threading.Thread(target=submit) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert set(versions) == {1}
