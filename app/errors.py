"""Distinguishable validation and runtime errors."""

from __future__ import annotations


class OrchestrationError(Exception):
    code = "ORCHESTRATION_ERROR"

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class EmptyDefinitionError(OrchestrationError):
    code = "EMPTY_DEFINITION"


class DuplicateTaskError(OrchestrationError):
    code = "DUPLICATE_TASK"


class SelfDependencyError(OrchestrationError):
    code = "SELF_DEPENDENCY"


class MissingDependencyError(OrchestrationError):
    code = "MISSING_DEPENDENCY"


class CyclicDependencyError(OrchestrationError):
    code = "CYCLIC_DEPENDENCY"


class InvalidActionError(OrchestrationError):
    code = "INVALID_ACTION"


class PipelineNotFoundError(OrchestrationError):
    code = "PIPELINE_NOT_FOUND"


class RunNotFoundError(OrchestrationError):
    code = "RUN_NOT_FOUND"


class RunNotCancellableError(OrchestrationError):
    code = "RUN_NOT_CANCELLABLE"


class InvalidQueryError(OrchestrationError):
    code = "INVALID_QUERY"
