"""业务六：HTTP API（校验错误码、幂等响应、取消与查询）。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app
from app.orchestrator import Orchestrator


def _client() -> tuple[TestClient, Orchestrator]:
    orch = Orchestrator(poll_interval=0.01)
    orch.start()
    return TestClient(create_app(orch)), orch


def test_publish_and_trigger_flow():
    client, orch = _client()
    with client:
        resp = client.post(
            "/workflows",
            json={"name": "p", "tasks": [{"name": "a"}, {"name": "b", "dependencies": ["a"]}]},
        )
        assert resp.status_code == 201
        assert resp.json()["workflow"]["version"] == 1

        resp = client.post("/workflows/p/runs", json={})
        assert resp.status_code == 201
        run = resp.json()["run"]

        import time

        for _ in range(200):
            state = client.get(f"/runs/{run['run_id']}").json()["state"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.02)
        assert state == "SUCCEEDED"


def test_api_returns_distinct_error_codes():
    client, orch = _client()
    with client:
        resp = client.post("/workflows", json={"name": "p", "tasks": []})
        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "EMPTY_WORKFLOW"

        resp = client.post(
            "/workflows",
            json={"name": "p", "tasks": [{"name": "a", "dependencies": ["x"]}]},
        )
        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "MISSING_DEPENDENCY"

        resp = client.post(
            "/workflows",
            json={"name": "p", "tasks": [{"name": "a", "dependencies": ["a"]}]},
        )
        assert resp.json()["detail"]["code"] == "SELF_DEPENDENCY"


def test_api_idempotent_trigger_and_cancel():
    client, orch = _client()
    with client:
        client.post("/workflows", json={"name": "p", "tasks": [{"name": "a"}, {"name": "b", "dependencies": ["a"]}]})
        first = client.post(
            "/workflows/p/runs",
            json={"idempotency_key": "k", "simulations": {"a": {"duration_seconds": 0.3}}},
        ).json()
        second = client.post(
            "/workflows/p/runs", json={"idempotency_key": "k"}
        ).json()

        assert first["run"]["run_id"] == second["run"]["run_id"]
        assert first["deduplicated"] is False
        assert second["deduplicated"] is True

        run_id = first["run"]["run_id"]
        resp = client.post(f"/runs/{run_id}/cancel")
        assert resp.status_code == 200

        import time

        for _ in range(200):
            state = client.get(f"/runs/{run_id}").json()["state"]
            if state in ("CANCELLED", "FAILED"):
                break
            time.sleep(0.02)
        assert state in ("CANCELLED", "FAILED")
