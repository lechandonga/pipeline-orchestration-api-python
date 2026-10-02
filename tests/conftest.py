"""Test fixtures and business-coverage logging.

Each test marks the business scenario it covers. The hooks below print a
``[业务覆盖]`` line per case and a grouped summary at the end, so unit test
logs show which businesses are covered and how each case fared.
"""

from __future__ import annotations

import logging

import pytest

from app.api import create_app


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "business(label): 业务场景分组")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def orchestrator(client):
    return client.app.state.orchestrator


SIMPLE_PIPELINE = {
    "name": "etl",
    "default_max_retries": 1,
    "tasks": [
        {"name": "extract", "dependencies": [], "action": {"type": "succeed"}},
        {
            "name": "transform",
            "dependencies": ["extract"],
            "action": {"type": "sleep", "seconds": 0},
        },
        {"name": "load", "dependencies": ["transform"], "action": {"type": "succeed"}},
    ],
}


def submit(client, payload):
    response = client.post("/pipelines", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def start_and_wait(client, name, **kwargs):
    body = {"wait": True, "wait_timeout": 15}
    body.update(kwargs)
    response = client.post(f"/pipelines/{name}/runs", json=body)
    assert response.status_code == 202, response.text
    return response.json()


@pytest.fixture
def helpers():
    return type(
        "H",
        (),
        {
            "SIMPLE_PIPELINE": SIMPLE_PIPELINE,
            "submit": staticmethod(submit),
            "start_and_wait": staticmethod(start_and_wait),
        },
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    marker = item.get_closest_marker("business")
    label = marker.args[0] if marker else "未分组"
    logging.getLogger("tests").info(
        "[业务覆盖] 业务=%s 用例=%s 开始执行", label, item.name
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        marker = item.get_closest_marker("business")
        label = marker.args[0] if marker else "未分组"
        item._business_label = label


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.write_sep("=", "业务覆盖汇总")
    grouped: dict[str, list] = {}
    label_map = (
        {item.nodeid: item._business_label for item in config._items}
        if hasattr(config, "_items")
        else {}
    )
    for outcome, label_text in (
        ("passed", "通过"),
        ("failed", "失败"),
        ("skipped", "跳过"),
    ):
        for report in terminalreporter.stats.get(outcome, []):
            if report.when != "call" and not (
                outcome == "skipped" and report.when == "setup"
            ):
                continue
            label = label_map.get(report.nodeid, "未分组")
            case_name = report.nodeid.split("::")[-1]
            grouped.setdefault(label, []).append((case_name, label_text))
    for label in sorted(grouped):
        cases = grouped[label]
        passed = sum(1 for _, r in cases if r == "通过")
        terminalreporter.write_line(
            f"业务[{label}] 共 {len(cases)} 个用例，通过 {passed}："
        )
        for name, result in cases:
            terminalreporter.write_line(f"  - {result}: {name}")


def pytest_collection_finish(session):
    session.config._items = list(session.items)
