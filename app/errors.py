"""可区分的业务错误码与异常。"""

from enum import Enum

from fastapi import HTTPException


class ErrorCode(str, Enum):
    # 流程定义校验
    EMPTY_WORKFLOW = "EMPTY_WORKFLOW"
    INVALID_WORKFLOW_NAME = "INVALID_WORKFLOW_NAME"
    INVALID_TASK_NAME = "INVALID_TASK_NAME"
    DUPLICATE_TASK_NAME = "DUPLICATE_TASK_NAME"
    SELF_DEPENDENCY = "SELF_DEPENDENCY"
    MISSING_DEPENDENCY = "MISSING_DEPENDENCY"
    CYCLIC_DEPENDENCY = "CYCLIC_DEPENDENCY"
    INVALID_RETRY_CONFIG = "INVALID_RETRY_CONFIG"
    # 资源
    UNKNOWN_WORKFLOW = "UNKNOWN_WORKFLOW"
    UNKNOWN_VERSION = "UNKNOWN_VERSION"
    # 运行
    RUN_ALREADY_TERMINAL = "RUN_ALREADY_TERMINAL"


class DefinitionError(ValueError):
    """流程定义非法，携带可区分错误码与上下文。"""

    def __init__(self, code: ErrorCode, message: str, **details: object) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def http_exception(self) -> HTTPException:
        return HTTPException(
            status_code=422,
            detail={
                "code": self.code.value,
                "message": self.message,
                "details": self.details,
            },
        )
