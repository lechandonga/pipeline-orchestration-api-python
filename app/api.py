"""FastAPI 路由定义。"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.errors import DefinitionError, ErrorCode
from app.models import RunView, TriggerRunPayload, WorkflowDefPayload
from app.orchestrator import Orchestrator


def create_app(orch: Orchestrator | None = None) -> FastAPI:
    orchestrator = orch or Orchestrator()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        orchestrator.shutdown()

    app = FastAPI(
        title="本地数据管道编排服务",
        version="0.1.0",
        description="本地模拟任务执行的流程编排：版本化定义、幂等触发、取消与重试传播。",
        lifespan=lifespan,
    )

    @app.post("/workflows", status_code=201)
    def publish_workflow(payload: WorkflowDefPayload) -> dict:
        try:
            definition = orchestrator.registry.publish(payload)
        except DefinitionError as exc:
            raise exc.http_exception() from exc
        return {
            "message": "流程定义已发布（不可变）",
            "workflow": definition.view(),
        }

    @app.get("/workflows/{name}")
    def get_workflow(name: str) -> dict:
        try:
            return orchestrator.registry.describe(name)
        except DefinitionError as exc:
            raise exc.http_exception() from exc

    @app.post("/workflows/{name}/runs", status_code=201)
    def trigger_run(name: str, payload: TriggerRunPayload) -> dict:
        try:
            run, deduplicated = orchestrator.trigger(
                workflow_name=name,
                idempotency_key=payload.idempotency_key,
                version=payload.version,
                simulations=payload.simulations,
            )
        except DefinitionError as exc:
            raise exc.http_exception() from exc
        view = run.view()
        return {
            "message": "重复幂等触发，返回既有运行" if deduplicated else "运行已创建",
            "deduplicated": deduplicated,
            "run": view.model_dump(mode="json"),
        }

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> RunView:
        try:
            return orchestrator.get_run(run_id).view()
        except DefinitionError as exc:
            raise exc.http_exception() from exc

    @app.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str) -> RunView:
        try:
            run = orchestrator.cancel(run_id)
        except DefinitionError as exc:
            status = 409 if exc.code == ErrorCode.RUN_ALREADY_TERMINAL else 422
            from fastapi import HTTPException

            raise HTTPException(
                status_code=status,
                detail={"code": exc.code.value, "message": exc.message, "details": exc.details},
            ) from exc
        return run.view()

    return app
