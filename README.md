# 本地数据管道编排服务

纯本地运行的流程编排 API：用内存存储 + 模拟任务执行器代替真实外部系统，
支持流程定义版本化、并发运行、幂等触发、取消、重试与失败传播。

## 启动

```bash
uv sync
uv run uvicorn main:app --reload
# API 文档： http://127.0.0.1:8000/docs
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines` | 提交/发布流程定义（校验失败返回 422，错误体含可区分 `code`） |
| GET | `/pipelines/{name}?version=` | 查询定义（默认最新版） |
| GET | `/pipelines/{name}/versions` | 列出全部已发布版本 |
| POST | `/pipelines/{name}/runs` | 发起运行，可带 `version`、`idempotency_key`、`wait` |
| GET | `/runs/{run_id}` | 查询运行状态（含每个任务的尝试记录） |
| GET | `/runs?pipeline=` | 列出运行 |
| POST | `/runs/{run_id}/wait?timeout=` | 等待运行进入终态 |
| POST | `/runs/{run_id}/cancel` | 取消运行（等待在途任务收敛后返回） |

### 定义示例

```json
{
  "name": "etl",
  "default_max_retries": 2,
  "tasks": [
    {"name": "extract", "dependencies": [], "action": {"type": "succeed"}},
    {"name": "transform", "dependencies": ["extract"],
     "action": {"type": "flaky", "succeed_on_attempt": 2}, "max_retries": 3},
    {"name": "load", "dependencies": ["transform"], "action": {"type": "succeed"}}
  ]
}
```

模拟动作：`succeed`（立即成功）、`fail`（每次必失败，带 `message`）、
`flaky`（第 `succeed_on_attempt` 次尝试起成功）、`sleep`（休眠 `seconds`
后成功，取消时可被即时中断）。

## 状态流转规则

任务状态：

```
PENDING ──> RUNNING ──> SUCCEEDED
   │          │
   │          └──> CANCELLED        （取消时正在执行，被中断/随取消收敛）
   ├──> FAILED                      （重试上限耗尽）
   └──> SKIPPED                     （上游 FAILED/SKIPPED/CANCELLED，
                                      或运行取消时尚未开始）
```

- 运行状态：`RUNNING -> SUCCEEDED | FAILED | CANCELLED`，均为终态。
- 仅当所有任务都进入终态后，运行才会进入终态；接口不会返回“运行已失败
  但仍有任务 RUNNING”这类自相矛盾的结果。
- 任一任务 FAILED 则运行 FAILED；其传递闭包下游标记 SKIPPED（`error`
  给出阻塞来源），SKIPPED 任务尝试次数为 0，从不执行也不计入成功。
- 与失败任务无依赖关系的分支继续执行，不受影响。
- 取消：未开始的任务立即 SKIPPED 且永不再被调度；进行中的任务通过
  cancel 事件中断并收敛到 CANCELLED；`/cancel` 等待全部任务收敛后再返回。
  已处于终态的运行再次取消返回 `409 RUN_NOT_CANCELLABLE`。

## 幂等约定

- 幂等作用域为 `(流程名, idempotency_key)`。相同 key 的重复触发（含并发
  触发）只创建一个运行，后续请求返回同一 `run_id` 与同一结果，
  响应中的 `created=false` 表示是回放。
- 幂等绑定在运行创建时确定，运行终态后 key 依然有效（迟到的重复触发
  返回历史结果）。
- 已完成的任务不会因重试调度或重复触发而再次执行；每个任务的
  `attempts` 完整记录每一次尝试（序号、状态、起止时间、错误信息）。

## 重试约定

- 重试次数 = `max_retries`，总尝试次数 = `max_retries + 1`。
- 任务级 `max_retries` 缺省时继承流程级 `default_max_retries`（默认 0）。
- 每次尝试（成功或失败）都落一条 attempt 记录；达到上限后任务 FAILED，
  错误信息包含总尝试次数与末次错误。

## 版本兼容范围

- 定义发布后不可修改：重复提交相同图结构返回同一版本（`reused=true`），
  图有任何变化（任务、依赖、动作、重试配置）则生成递增新版本。
- 每个运行在创建时固化定义快照（响应中的 `version`），历史运行永远按
  发起时的版本解释与展示；之后提交新版本不改变旧运行的任何结论。
- 发起运行时可用 `version` 显式指定历史版本；不指定则使用最新版本。

非法定义的错误码（HTTP 422，`detail.code`）：`EMPTY_DEFINITION`、
`DUPLICATE_TASK`、`SELF_DEPENDENCY`、`MISSING_DEPENDENCY`、
`CYCLIC_DEPENDENCY`、`INVALID_ACTION`。资源不存在为 404
（`PIPELINE_NOT_FOUND` / `RUN_NOT_FOUND`）。

## 本地验证

```bash
uv run pytest                 # 运行全部单测
uv run pytest -q              # 安静模式，末尾打印“业务覆盖汇总”
uv run pytest -k 取消          # 只跑取消相关业务
```

测试日志包含两类业务可读信息：

- 每个用例开始时输出 `[业务覆盖] 业务=<分组> 用例=<名称> 开始执行`，
  运行过程中编排器输出创建、尝试、失败、跳过、取消等事件日志；
- 结束时输出“业务覆盖汇总”，按业务分组列出用例及通过/失败情况，
  覆盖：非法定义（空/重名/自依赖/缺依赖/成环/非法动作/错误可区分）、
  定义版本化、调度顺序、重复与并发幂等、同流程并发运行、取消、
  重试记录、失败传播、成功任务不重复记账、跨版本历史结论稳定性。
