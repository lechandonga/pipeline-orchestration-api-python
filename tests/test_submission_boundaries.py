"""业务：提交边界——名称归一化、版本连续唯一、相同定义复用。"""

import pytest


@pytest.mark.business("提交边界-流程名前后空格归一化")
def test_pipeline_name_whitespace_is_normalized(client):
    payload = {
        "name": "  spaced  ",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    first = client.post("/pipelines", json=payload).json()
    assert first["name"] == "spaced"
    again = client.post("/pipelines", json=payload).json()
    assert again["name"] == "spaced"
    assert again["version"] == 1
    assert again["reused"] is True
    # 触发运行时带空格也算同一个流程
    run = client.post(
        "/pipelines/%20%20spaced%20/runs", json={"wait": True, "wait_timeout": 10}
    ).json()
    assert run["pipeline"] == "spaced"
    assert run["state"] == "SUCCEEDED"


@pytest.mark.business("提交边界-版本号连续唯一")
def test_version_numbers_are_sequential_and_unique(client):
    base = {"tasks": [{"name": "a", "dependencies": []}]}
    versions = []
    for extra in ([], [{"name": "b", "dependencies": ["a"]}], [
        {"name": "b", "dependencies": ["a"]},
        {"name": "c", "dependencies": []},
    ]):
        payload = {"name": "seq", "tasks": [base["tasks"][0], *extra]}
        body = client.post("/pipelines", json=payload).json()
        versions.append(body["version"])
    assert versions == [1, 2, 3]
    listed = client.get("/pipelines/seq/versions").json()["versions"]
    assert [v["version"] for v in listed] == [1, 2, 3]


@pytest.mark.business("提交边界-重复提交相同定义复用原版本")
def test_repeated_identical_submission_reuses_version_even_after_changes(client):
    v1 = {"name": "back", "tasks": [{"name": "a", "dependencies": []}]}
    v2 = {
        "name": "back",
        "tasks": [
            {"name": "a", "dependencies": []},
            {"name": "b", "dependencies": ["a"]},
        ],
    }
    client.post("/pipelines", json=v1)
    client.post("/pipelines", json=v2)
    replayed = client.post("/pipelines", json=v1).json()
    assert replayed["version"] == 1
    assert replayed["reused"] is True


@pytest.mark.business("提交边界-超时配置属于定义差异")
def test_timeout_change_creates_new_version(client):
    payload = {
        "name": "to-version",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    first = client.post("/pipelines", json=payload).json()
    payload["tasks"][0]["timeout_seconds"] = 3
    second = client.post("/pipelines", json=payload).json()
    assert first["version"] == 1
    assert second["version"] == 2
    assert second["tasks"][0]["timeout_seconds"] == 3
    third = client.post("/pipelines", json=payload).json()
    assert third["version"] == 2
    assert third["reused"] is True


@pytest.mark.business("提交边界-非法超时参数被拒")
def test_invalid_task_timeout_rejected(client):
    payload = {
        "name": "bad-timeout",
        "tasks": [{"name": "a", "dependencies": [], "timeout_seconds": -1}],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_ACTION"
