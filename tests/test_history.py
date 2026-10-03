"""业务：历史运行过滤、分页与汇总统计的一致性。"""

import time

import pytest


def make_pipeline(name):
    return {
        "name": name,
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": "succeed"}},
            {"name": "b", "dependencies": ["a"], "action": {"type": "succeed"}},
        ],
    }


@pytest.mark.business("历史查询-按流程名与状态过滤")
def test_filter_by_pipeline_and_state(client):
    client.post("/pipelines", json=make_pipeline("alpha"))
    client.post("/pipelines", json=make_pipeline("beta"))
    for _ in range(3):
        client.post(
            "/pipelines/alpha/runs", json={"wait": True, "wait_timeout": 10}
        )
    client.post("/pipelines/beta/runs", json={"wait": True, "wait_timeout": 10})

    alpha = client.get("/runs?pipeline=alpha").json()
    assert alpha["total"] == 3
    assert {r["pipeline"] for r in alpha["runs"]} == {"alpha"}

    succeeded = client.get("/runs?pipeline=alpha&state=SUCCEEDED").json()
    assert succeeded["total"] == 3
    assert all(r["state"] == "SUCCEEDED" for r in succeeded["runs"])

    queued = client.get("/runs?state=QUEUED").json()
    assert queued["total"] == 0


@pytest.mark.business("历史查询-时间范围过滤")
def test_filter_by_time_range(client):
    client.post("/pipelines", json=make_pipeline("timed"))
    before = time.time() - 1
    client.post(
        "/pipelines/timed/runs", json={"wait": True, "wait_timeout": 10}
    )
    after = time.time() + 1
    assert client.get(f"/runs?created_from={before}").json()["total"] >= 1
    assert client.get(f"/runs?created_to={after}").json()["total"] >= 1
    assert client.get(f"/runs?created_from={after}").json()["total"] == 0
    assert client.get(f"/runs?created_to={before}").json()["total"] == 0


@pytest.mark.business("历史查询-分页不重不漏且顺序稳定")
def test_pagination_is_stable_and_complete(client):
    client.post("/pipelines", json=make_pipeline("paged"))
    for _ in range(7):
        client.post(
            "/pipelines/paged/runs", json={"wait": True, "wait_timeout": 10}
        )
    seen: list[str] = []
    for offset in range(0, 7, 3):
        page = client.get(f"/runs?pipeline=paged&limit=3&offset={offset}").json()
        assert page["total"] == 7
        assert page["offset"] == offset
        seen.extend(r["run_id"] for r in page["runs"])
    assert len(seen) == 7
    assert len(set(seen)) == 7


@pytest.mark.business("历史统计-数字与明细对得上")
def test_stats_match_details(client):
    client.post("/pipelines", json=make_pipeline("stats"))
    for _ in range(4):
        client.post(
            "/pipelines/stats/runs", json={"wait": True, "wait_timeout": 10}
        )
    # one failed run
    client.post(
        "/pipelines",
        json={
            "name": "stats",
            "tasks": [
                {"name": "a", "dependencies": [], "action": {"type": "fail"}},
            ],
        },
    )
    client.post(
        "/pipelines/stats/runs", json={"wait": True, "wait_timeout": 10}
    )
    stats = client.get("/runs/stats?pipeline=stats").json()
    details = client.get("/runs?pipeline=stats&limit=200").json()
    counts: dict[str, int] = {}
    durations = []
    for run in details["runs"]:
        counts[run["state"]] = counts.get(run["state"], 0) + 1
        if run["duration_seconds"] is not None:
            durations.append(run["duration_seconds"])
    assert stats["total"] == details["total"] == 5
    assert stats["by_state"]["SUCCEEDED"] == counts["SUCCEEDED"] == 4
    assert stats["by_state"]["FAILED"] == counts["FAILED"] == 1
    assert stats["completed"] == 5
    assert stats["average_duration_seconds"] == pytest.approx(
        sum(durations) / len(durations)
    )
    assert stats["queued"] == 0


@pytest.mark.business("历史查询-上百条历史仍然快速返回")
def test_history_query_performance(client):
    client.post("/pipelines", json=make_pipeline("many"))
    for _ in range(120):
        client.post("/pipelines/many/runs", json={"wait": True, "wait_timeout": 10})
    started = time.time()
    page = client.get("/runs?pipeline=many&state=SUCCEEDED&limit=20&offset=50").json()
    stats = client.get("/runs/stats").json()
    elapsed = time.time() - started
    assert page["total"] == 120
    assert len(page["runs"]) == 20
    assert stats["by_state"]["SUCCEEDED"] == 120
    assert elapsed < 1.0


@pytest.mark.business("历史查询-非法状态参数返回 422")
def test_invalid_state_filter_rejected(client):
    response = client.get("/runs?state=BOGUS")
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_STATE"


@pytest.mark.business("历史查询-过滤分页不影响进行中的调度")
def test_queries_do_not_block_scheduling(client):
    client.post(
        "/pipelines",
        json={
            "name": "live",
            "tasks": [
                {"name": "a", "dependencies": [], "action": {"type": "sleep", "seconds": 0.4}},
            ],
        },
    )
    live = client.post("/pipelines/live/runs", json={}).json()["run_id"]
    client.post("/pipelines", json=make_pipeline("many2"))
    for _ in range(60):
        client.post(
            "/pipelines/many2/runs", json={"wait": True, "wait_timeout": 10}
        )
    for _ in range(10):
        client.get("/runs?limit=200")
        client.get("/runs/stats")
    run = client.post(f"/runs/{live}/wait?timeout=10", json={}).json()
    assert run["state"] == "SUCCEEDED"
