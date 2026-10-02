"""业务：流程定义提交与校验。"""

import pytest


@pytest.mark.business("非法定义-空定义")
def test_empty_tasks_rejected(client, helpers):
    response = client.post("/pipelines", json={"name": "p", "tasks": []})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "EMPTY_DEFINITION"


@pytest.mark.business("非法定义-空定义")
def test_missing_name_rejected(client):
    response = client.post("/pipelines", json={"tasks": [{"name": "a"}]})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "EMPTY_DEFINITION"


@pytest.mark.business("非法定义-任务重名")
def test_duplicate_task_name_rejected(client):
    payload = {
        "name": "dup",
        "tasks": [
            {"name": "a", "dependencies": []},
            {"name": "a", "dependencies": []},
        ],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "DUPLICATE_TASK"
    assert detail["duplicates"] == ["a"]


@pytest.mark.business("非法定义-自依赖")
def test_self_dependency_rejected(client):
    payload = {
        "name": "self",
        "tasks": [{"name": "a", "dependencies": ["a"]}],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "SELF_DEPENDENCY"


@pytest.mark.business("非法定义-依赖缺失")
def test_missing_dependency_rejected(client):
    payload = {
        "name": "missing",
        "tasks": [{"name": "a", "dependencies": ["ghost"]}],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "MISSING_DEPENDENCY"
    assert detail["missing"] == {"a": ["ghost"]}


@pytest.mark.business("非法定义-依赖成环")
def test_cyclic_dependency_rejected(client):
    payload = {
        "name": "cycle",
        "tasks": [
            {"name": "a", "dependencies": ["c"]},
            {"name": "b", "dependencies": ["a"]},
            {"name": "c", "dependencies": ["b"]},
        ],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "CYCLIC_DEPENDENCY"
    assert set(detail["tasks_in_cycle"]) == {"a", "b", "c"}


@pytest.mark.business("非法定义-动作参数非法")
def test_unknown_action_rejected(client):
    payload = {
        "name": "bad-action",
        "tasks": [{"name": "a", "action": {"type": "explode"}}],
    }
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_ACTION"


@pytest.mark.business("非法定义-校验顺序可区分")
def test_distinguishable_error_codes(client):
    # 自依赖先于依赖缺失被检出
    payload = {
        "name": "order",
        "tasks": [{"name": "a", "dependencies": ["a", "ghost"]}],
    }
    response = client.post("/pipelines", json=payload)
    assert response.json()["detail"]["code"] == "SELF_DEPENDENCY"


@pytest.mark.business("定义版本化-发布不可变")
def test_repost_same_graph_reuses_version(client):
    payload = {
        "name": "v",
        "tasks": [{"name": "a", "dependencies": [], "action": {"type": "succeed"}}],
    }
    first = client.post("/pipelines", json=payload).json()
    second = client.post("/pipelines", json=payload).json()
    assert first["version"] == 1
    assert second["version"] == 1
    assert second["reused"] is True


@pytest.mark.business("定义版本化-发布不可变")
def test_changed_graph_creates_new_version(client):
    payload_v1 = {
        "name": "v",
        "tasks": [{"name": "a", "dependencies": []}],
    }
    payload_v2 = {
        "name": "v",
        "tasks": [
            {"name": "a", "dependencies": []},
            {"name": "b", "dependencies": ["a"]},
        ],
    }
    v1 = client.post("/pipelines", json=payload_v1).json()
    v2 = client.post("/pipelines", json=payload_v2).json()
    assert v1["version"] == 1
    assert v2["version"] == 2
    assert v2["reused"] is False
    versions = client.get("/pipelines/v/versions").json()["versions"]
    assert [v["version"] for v in versions] == [1, 2]
    assert len(versions[0]["tasks"]) == 1
