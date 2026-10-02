# 本地数据管道编排服务（Python / FastAPI）

一个纯本地运行的数据管道编排服务：流程由带依赖关系的任务组成，任务执行由
`LocalTaskExecutor` 模拟（可注入耗时与失败次数），不依赖任何真实外部系统。
提供流程定义的版本化发布、运行的并发触发、幂等去重、取消、重试与失败传播。

## 快速开始

```bash
uv sync --group dev          # 安装运行与测试依赖
uv run python main.py        # 启动 HTTP 服务（uvicorn）
uv run pytest                # 运行全部单测，日志实时展示业务与用例
uv run pytest -v             # 同时展示每个用例的 PASSED/FAILED
```

## HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /workflows` | 提交并校验流程定义；合法则发布为一个**不可变新版本** |
| `GET /workflows/{name}` | 查看流程全部历史版本 |
| `POST /workflows/{name}/runs` | 触发运行，可带 `idempotency_key`、`version`、`simulations` |
| `GET /runs/{run_id}` | 查询运行快照（状态、每个任务的尝试记录） |
| `POST /runs/{run_id}/cancel` | 请求取消运行 |

### 示例

```bash
curl -X POST localhost:8000/workflows -H 'content-type: application/json' -d '{
  "name": "etl",
  "tasks": [
    {"name": "extract"},
    {"name": "transform", "dependencies": ["extract"], "retries": 2},
    {"name": "load", "dependencies": ["transform"]}
  ]
}'

# 触发运行：transform 的前 1 次尝试模拟失败（用重试覆盖），每次尝试耗时 10ms
curl -X POST localhost:8000/workflows/etl/runs -H 'content-type: application/json' -d '{
  "idempotency_key": "daily-2026-10-02",
  "simulations": {"transform": {"fail_times": 1, "duration_seconds": 0.01}}
}'
```

`simulations` 仅用于本地模拟，按运行传入、不写回定义：

- `fail_times`：该任务**前 N 次尝试**抛出模拟失败；
- `duration_seconds`：每次尝试的模拟耗时，期间可被取消中断。

## 状态流转规则

任务状态（`TaskState`）：

```
PENDING ──依赖全部成功且开始执行──▶ RUNNING ──成功──▶ SUCCESS
   │                                 │
   │                                 ├── 尝试达到上限仍失败 ──▶ FAILED
   │                                 └── 执行中/重试等待中收到取消 ──▶ CANCELLED
   ├── 上游 FAILED/SKIPPED/CANCELLED ──▶ SKIPPED（从不启动，零尝试记录）
   └── 取消时尚未开始 ──▶ CANCELLED
```

运行状态（`RunState`）：

```
PENDING ──▶ RUNNING ──所有任务终态──▶ SUCCEEDED（全部 SUCCESS）
                        └───────────▶ FAILED（任一任务 FAILED，或出现 SKIPPED/CANCELLED）
PENDING/RUNNING ──取消请求──▶ CANCELLING ──所有任务终态──▶ CANCELLED / FAILED
```

收敛约定：

- 取消后**未开始**的任务不再启动，标记为 `CANCELLED`（或因上游未成功而 `SKIPPED`）；
- **进行中**的任务通过可中断等待感知取消信号，最终收敛到 `CANCELLED`，不留下 `RUNNING`/`PENDING`；
- 运行只在所有任务都进入终态后才进入终态，并记录 `finished_at`；
- 终态运行重复取消返回 `409 RUN_ALREADY_TERMINAL`。

## 幂等约定

- 幂等域为 `(workflow_name, idempotency_key)`，在注册表范围内全局生效，与并发无关；
- 首次触发生成唯一 `run_id`；后续相同键的触发（**包括并发触发**）只返回同一个运行，
  响应中 `deduplicated=true`；
- 已完成的运行重复触发返回原运行与原结论，任务**不会重复执行**（尝试记录不增加）；
- 不传 `idempotency_key` 的触发每次都产生独立新运行。

## 重试与失败传播约定

- 每个任务用 `retries` 配置**额外**重试次数（默认 0，即只尝试一次），
  总尝试次数 = `retries + 1`；`retry_delay_seconds` 配置退避等待；
- 每次尝试都记录 `attempt / status(FAILED|SUCCESS|CANCELLED) / error / started_at / finished_at`；
- 达到重试上限：任务标记 `FAILED` 且 reason 注明尝试耗尽，
  其直接、间接下游标记 `SKIPPED`，**不执行、不记账、不计入成功**；
- 与失败分支无依赖关系的任务不受影响，可继续成功；
- 重试等待或执行期间收到取消，任务收敛为 `CANCELLED`，不再继续重试。

## 版本兼容范围

- 定义一经发布即不可变：再次 `POST /workflows` 只会追加新版本（v1、v2 …），
  历史版本的任务集合、依赖与重试配置永不改变；
- 每个运行在**触发时刻**绑定定义快照（响应含 `definition_version`）；
  触发后提交新版本不影响任何已有运行，旧运行始终按发起时的版本解释；
- 触发时可用 `version` 指定历史版本，不传则使用当前最新版本；
- 运行状态与尝试记录保存在内存中，服务重启后清空；本项目的兼容范围限于
  单进程内存态，不涉及跨进程持久化或分布式协调。

## 定义校验错误码

非法定义返回 `422`，`detail.code` 可区分：`EMPTY_WORKFLOW`、
`INVALID_WORKFLOW_NAME`、`INVALID_TASK_NAME`、`DUPLICATE_TASK_NAME`、
`SELF_DEPENDENCY`、`MISSING_DEPENDENCY`、`CYCLIC_DEPENDENCY`（返回环路径）、
`INVALID_RETRY_CONFIG`；资源类：`UNKNOWN_WORKFLOW`、`UNKNOWN_VERSION`。

## 测试与业务覆盖

`uv run pytest` 会输出实时业务日志（每个用例开始/完成、运行终态、失败尝试等）。
用例按业务文件分组：

| 文件 | 覆盖业务 |
| --- | --- |
| `tests/test_definitions.py` | 合法 DAG、空流程/重名/自依赖/缺依赖/成环错误码、不可变快照、版本并存 |
| `tests/test_trigger_idempotency.py` | 基础成功、同键幂等、异键隔离、5 线程并发同键去重、版本绑定、终态不重复执行 |
| `tests/test_concurrency.py` | 并发运行相互独立、无依赖任务并行、下游等待上游时间戳 |
| `tests/test_cancel.py` | 启动前取消、执行中取消收敛、终态取消拒绝、取消不影响其他运行 |
| `tests/test_retry_and_failure.py` | 多次尝试记录、重试上限失败与全下游跳过、零重试、幂等不重跑、菱形分支传播 |
| `tests/test_api.py` | HTTP 发布/触发/查询、可区分错误码、幂等响应、取消接口 |

## 代码结构

```
app/
  models.py        # 状态枚举与 Pydantic 模型
  errors.py        # 错误码与 DefinitionError
  definition.py    # DAG 校验与不可变版本注册表
  executor.py      # 本地模拟执行器（耗时、失败注入、可取消等待）
  orchestrator.py  # 调度线程 + worker 线程：依赖推进/取消/重试/幂等
  api.py           # FastAPI 路由
tests/             # 按业务分组的 pytest 用例
main.py            # 入口，uvicorn 启动 app
```
