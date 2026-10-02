"""业务一：流程定义提交与校验（非法定义必须被拒绝且错误原因可区分）。"""

from __future__ import annotations

import pytest

from app.errors import DefinitionError, ErrorCode
from app.models import TaskDef, WorkflowDefPayload
from tests.conftest import make_workflow


def test_accept_valid_dag(orchestrator):
    version = orchestrator.registry.publish(
        make_workflow("valid", [("a", []), ("b", ["a"]), ("c", ["a", "b"])])
    ).version
    assert version == 1


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        (WorkflowDefPayload(name="empty", tasks=[]), ErrorCode.EMPTY_WORKFLOW),
        (WorkflowDefPayload(name="", tasks=[TaskDef(name="a")]), ErrorCode.INVALID_WORKFLOW_NAME),
        (
            make_workflow("dup", [("a", []), ("a", [])]),
            ErrorCode.DUPLICATE_TASK_NAME,
        ),
        (
            make_workflow("self", [("a", ["a"])]),
            ErrorCode.SELF_DEPENDENCY,
        ),
        (
            make_workflow("missing", [("a", ["ghost"])]),
            ErrorCode.MISSING_DEPENDENCY,
        ),
        (
            make_workflow("cycle", [("a", ["b"]), ("b", ["a"])]),
            ErrorCode.CYCLIC_DEPENDENCY,
        ),
        (
            make_workflow("cycle3", [("a", ["c"]), ("b", ["a"]), ("c", ["b"])]),
            ErrorCode.CYCLIC_DEPENDENCY,
        ),
    ],
)
def test_invalid_definitions_rejected_with_distinct_codes(orchestrator, payload, expected_code):
    with pytest.raises(DefinitionError) as exc_info:
        orchestrator.registry.publish(payload)
    assert exc_info.value.code == expected_code


def test_published_definitions_are_immutable(orchestrator):
    payload = make_workflow("frozen", [("a", []), ("b", ["a"])])
    definition = orchestrator.registry.publish(payload)

    # 外部对象再被修改，不影响已发布快照
    payload.tasks[0].name = "hacked"
    payload.tasks.append(TaskDef(name="extra"))

    assert [task.name for task in definition.tasks] == ["a", "b"]
    assert orchestrator.registry.get("frozen", 1).tasks_by_name["a"].dependencies == []


def test_new_version_does_not_replace_old(orchestrator):
    orchestrator.registry.publish(make_workflow("ver", [("a", [])]))
    orchestrator.registry.publish(make_workflow("ver", [("a", []), ("b", ["a"])]))

    v1 = orchestrator.registry.get("ver", 1)
    latest = orchestrator.registry.get("ver")
    assert v1.version == 1 and len(v1.tasks) == 1
    assert latest.version == 2 and len(latest.tasks) == 2


def test_unknown_workflow_and_version(orchestrator):
    with pytest.raises(DefinitionError) as missing:
        orchestrator.registry.get("nope")
    assert missing.value.code == ErrorCode.UNKNOWN_WORKFLOW

    orchestrator.registry.publish(make_workflow("v", [("a", [])]))
    with pytest.raises(DefinitionError) as bad_version:
        orchestrator.registry.get("v", 9)
    assert bad_version.value.code == ErrorCode.UNKNOWN_VERSION
