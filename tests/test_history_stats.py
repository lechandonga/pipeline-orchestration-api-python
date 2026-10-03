"""业务：历史过滤、分页与汇总统计的一致性。"""

from __future__ import annotations

import time

import pytest


def _make_pipeline(name, action="succeed"):
    return {
        "name": name,
        "tasks": [
            {"name": "a", "dependencies": [], "action": {"type": action}},
        ],
    }


def _wait_all(client, run_ids):
    for run_id in run_ids:
        client.post(f"/runs/{run_id}/wait?timeout=15", json={})


@pytest.mark.business("历史查询-按流程名状态时间范围过滤")
def test_filters_by_pipeline_state_and_time(client, helpers):
    helpers.submit(client, _make_pipeline("ok"))
    helpers.submit(client, _make_pipeline("boom", action="fail"))
    ok_ids = [
        client.post("/pipelines/ok/runs", json={}).json()["run_id"] for _ in range(3)
    ]
    fail_ids = [
        client.post("/pipelines/boom/runs", json={}).json()["run_id"]
        for _ in range(2)
    ]
    _wait_all(client, ok_ids + fail_ids)

    ok_rows = client.get("/runs?pipeline=ok&state=SUCCEEDED").json()
    assert ok_rows["total"] == 3
    assert {r["run_id"] for r in ok_rows["runs"]} == set(ok_ids)

    failed = client.get("/runs?state=FAILED").json()
    assert failed["total"] == 2
    assert {r["run_id"] for r in failed["runs"]} == set(fail_ids)

    now = time.time()
    past = client.get(
        "/runs?start_time=0&end_time=" + str(now + 1000)
    ).json()
    assert past["total"] == 5
    future = client.get("/runs?start_time=" + str(now + 1000)).json()
    assert future["total"] == 0


@pytest.mark.business("历史查询-分页不重不漏")
def test_pagination_exact_and_stable(client, helpers):
    helpers.submit(client, _make_pipeline("page"))
    ids = [
        client.post("/pipelines/page/runs", json={}).json()["run_id"]
        for _ in range(7)
    ]
    _wait_all(client, ids)
    seen: list[str] = []
    for offset in range(0, 7, 3):
        body = client.get(
            f"/runs?pipeline=page&limit=3&offset={offset}"
        ).json()
        assert body["total"] == 7
        seen.extend(r["run_id"] for r in body["runs"])
    assert seen == ids  # 触发顺序、不重不漏


@pytest.mark.business("历史统计-与过滤明细对得上")
def test_stats_reconcile_with_details(client, helpers):
    helpers.submit(client, _make_pipeline("stat"))
    ids = [
        client.post("/pipelines/stat/runs", json={}).json()["run_id"]
        for _ in range(4)
    ]
    _wait_all(client, ids)
    stats = client.get("/runs-stats?pipeline=stat").json()
    assert stats["states"]["SUCCEEDED"] == 4
    assert stats["finished"] == 4
    assert stats["queued"] == 0
    assert stats["average_duration_seconds"] is not None
    assert stats["average_duration_seconds"] >= 0
    # 统计总数等于各状态明细之和
    total_from_list = 0
    for state in stats["states"]:
        total_from_list += client.get(
            f"/runs?pipeline=stat&state={state}"
        ).json()["total"]
    assert total_from_list == stats["total"] == 4


@pytest.mark.business("历史统计-平均耗时与逐条明细一致")
def test_average_duration_matches_rows(client, helpers):
    helpers.submit(client, _make_pipeline("dur"))
    ids = [
        client.post("/pipelines/dur/runs", json={}).json()["run_id"]
        for _ in range(3)
    ]
    _wait_all(client, ids)
    rows = client.get("/runs?pipeline=dur&limit=10").json()["runs"]
    durations = [r["wall_finished_at"] - r["wall_started_at"] for r in rows]
    stats = client.get("/runs-stats?pipeline=dur").json()
    assert stats["average_duration_seconds"] == pytest.approx(
        sum(durations) / len(durations)
    )


@pytest.mark.business("历史查询-非法状态过滤报错")
def test_invalid_state_filter(client):
    response = client.get("/runs?state=NOPE")
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_QUERY"


@pytest.mark.business("历史查询-上百运行查询仍快速")
def test_query_fast_with_hundreds_of_runs(client, helpers):
    helpers.submit(client, _make_pipeline("many"))
    ids = [
        client.post("/pipelines/many/runs", json={}).json()["run_id"]
        for _ in range(150)
    ]
    _wait_all(client, ids)
    start = time.perf_counter()
    body = client.get("/runs?pipeline=many&state=SUCCEEDED&limit=20&offset=100").json()
    elapsed = time.perf_counter() - start
    assert body["total"] == 150
    assert len(body["runs"]) == 20
    assert elapsed < 1.0
    stats_start = time.perf_counter()
    stats = client.get("/runs-stats").json()
    assert time.perf_counter() - stats_start < 1.0
    assert stats["total"] >= 150
