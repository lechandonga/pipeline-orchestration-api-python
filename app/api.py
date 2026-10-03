"""FastAPI HTTP surface for the local orchestration service."""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from .errors import OrchestrationError
from .executor import LocalExecutor
from .models import RunState
from .orchestrator import Orchestrator, RunQuery
from .serialization import definition_to_dict, run_to_dict
from .validation import DefinitionRegistry

log = logging.getLogger("api")


class StartRunRequest(BaseModel):
    version: int | None = Field(default=None, ge=1)
    idempotency_key: str | None = Field(default=None, min_length=1)
    timeout_seconds: float | None = Field(default=None, gt=0)
    wait: bool = False
    wait_timeout: float | None = Field(default=None, ge=0)


def create_app() -> FastAPI:
    app = FastAPI(
        title="Local Pipeline Orchestration API",
        version="1.0.0",
        lifespan=_lifespan,
    )
    app.state.registry = DefinitionRegistry()
    app.state.executor = LocalExecutor()
    app.state.orchestrator = None  # created in lifespan

    _register_routes(app)
    return app


from contextlib import asynccontextmanager


@asynccontextmanager
async def _lifespan(app: FastAPI):
    max_concurrent = int(os.environ.get("MAX_CONCURRENT_RUNS", "8"))
    max_workers = int(os.environ.get("MAX_TASK_WORKERS", "64"))
    orchestrator = Orchestrator(
        app.state.registry,
        app.state.executor,
        max_workers=max_workers,
        max_concurrent_runs=max_concurrent,
    )
    app.state.orchestrator = orchestrator
    log.info(
        "orchestration service started max_concurrent_runs=%d max_workers=%d",
        max_concurrent,
        max_workers,
    )
    try:
        yield
    finally:
        orchestrator.shutdown()
        log.info("orchestration service stopped")


def _orch(request: Request) -> Orchestrator:
    return request.app.state.orchestrator


def _raise(exc: OrchestrationError) -> None:
    raise HTTPException(
        status_code=_status_for(exc.code),
        detail={"code": exc.code, "message": exc.message, **exc.details},
    )


def _status_for(code: str) -> int:
    if code in {"PIPELINE_NOT_FOUND", "RUN_NOT_FOUND"}:
        return 404
    if code == "RUN_NOT_CANCELLABLE":
        return 409
    return 422


def _register_routes(app: FastAPI) -> None:
    @app.post("/pipelines", status_code=201)
    def submit_pipeline(request: Request, payload: dict):
        try:
            result = _orch(request).registry.submit(payload)
        except OrchestrationError as exc:
            _raise(exc)
        body = definition_to_dict(result.definition)
        body["reused"] = result.reused
        log.info(
            "pipeline submitted name=%s version=%d reused=%s",
            result.name,
            result.version,
            result.reused,
        )
        return body

    @app.get("/pipelines/{name}")
    def get_pipeline(
        request: Request, name: str, version: int | None = Query(default=None, ge=1)
    ):
        try:
            definition = _orch(request).registry.get(name, version)
        except OrchestrationError as exc:
            _raise(exc)
        return definition_to_dict(definition)

    @app.get("/pipelines/{name}/versions")
    def list_versions(request: Request, name: str):
        try:
            versions = _orch(request).registry.list_versions(name)
        except OrchestrationError as exc:
            _raise(exc)
        return {"name": name, "versions": [definition_to_dict(v) for v in versions]}

    @app.post("/pipelines/{name}/runs", status_code=202)
    def start_run(request: Request, name: str, body: StartRunRequest | None):
        body = body or StartRunRequest()
        try:
            run, created = _orch(request).start_run(
                name,
                version=body.version,
                idempotency_key=body.idempotency_key,
                timeout_seconds=body.timeout_seconds,
            )
            if body.wait:
                run = _orch(request).wait_for(run.run_id, body.wait_timeout)
        except OrchestrationError as exc:
            _raise(exc)
        result = run_to_dict(run)
        result["created"] = created
        return result

    @app.get("/runs/stats")
    def run_stats(
        request: Request,
        pipeline: str | None = None,
        created_from: float | None = Query(default=None),
        created_to: float | None = Query(default=None),
    ):
        return _orch(request).run_stats(pipeline, created_from, created_to)

    @app.get("/runs/{run_id}")
    def get_run(request: Request, run_id: str):
        try:
            run = _orch(request).get_run(run_id)
        except OrchestrationError as exc:
            _raise(exc)
        return run_to_dict(run)


    @app.get("/runs")
    def list_runs(
        request: Request,
        pipeline: str | None = None,
        state: str | None = None,
        created_from: float | None = Query(default=None),
        created_to: float | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        if state is not None:
            state = state.strip().upper()
            valid = {s.value for s in RunState}
            if state not in valid:
                raise HTTPException(
                    status_code=422,
                    detail={"code": "INVALID_STATE", "message": f"unknown state '{state}'"},
                )
        page = _orch(request).query_runs(
            RunQuery(
                pipeline_name=pipeline,
                state=state,
                created_from=created_from,
                created_to=created_to,
                limit=limit,
                offset=offset,
            )
        )
        return {
            "runs": [run_to_dict(r) for r in page.runs],
            "total": page.total,
            "limit": page.limit,
            "offset": page.offset,
        }

    @app.post("/runs/{run_id}/wait")
    def wait_run(
        request: Request, run_id: str, timeout: float | None = Query(default=None, ge=0)
    ):
        try:
            run = _orch(request).wait_for(run_id, timeout)
        except OrchestrationError as exc:
            _raise(exc)
        return run_to_dict(run)

    @app.post("/runs/{run_id}/cancel", status_code=200)
    def cancel_run(request: Request, run_id: str):
        try:
            run = _orch(request).cancel_run(run_id)
            # Converge: wait for in-flight tasks before responding.
            _orch(request)._wait_settled(run, timeout=30)
            run = _orch(request).get_run(run_id)
        except OrchestrationError as exc:
            _raise(exc)
        return run_to_dict(run)

    @app.get("/health")
    def health():
        return {"status": "ok"}
