"""pytest 公共夹具：每个用例使用独立的编排器，并输出业务级日志。"""

from __future__ import annotations

import logging

import pytest

from app.models import TaskDef, WorkflowDefPayload
from app.orchestrator import Orchestrator

logger = logging.getLogger("pipeline.tests")


@pytest.fixture
def orchestrator() -> Orchestrator:
    orch = Orchestrator(poll_interval=0.01)
    orch.start()
    yield orch
    orch.shutdown()


@pytest.fixture(autouse=True)
def _log_case(request: pytest.FixtureRequest) -> None:
    module = request.module.__doc__ or request.module.__name__
    logger.info("==== 业务: %s | 用例: %s 开始 ====", module.strip(), request.node.name)
    yield
    logger.info("==== 业务: %s | 用例: %s 完成 ====", module.strip(), request.node.name)


def make_workflow(name: str, tasks: list[tuple[str, list[str]]] | list[dict]) -> WorkflowDefPayload:
    """便捷构造流程定义；元组形式或完整字典形式。"""
    defs: list[TaskDef] = []
    for item in tasks:
        if isinstance(item, tuple):
            defs.append(TaskDef(name=item[0], dependencies=list(item[1])))
        else:
            defs.append(TaskDef(**item))
    return WorkflowDefPayload(name=name, tasks=defs)


def publish(orch: Orchestrator, name: str, tasks) -> int:
    return orch.registry.publish(make_workflow(name, tasks)).version
