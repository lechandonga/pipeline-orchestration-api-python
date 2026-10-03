# 本地数据管道编排服务

纯本地运行的流程编排 API：用内存存储 + 模拟任务执行器代替真实外部系统，
支持流程定义版本化、批量并发与 FIFO 排队、幂等触发、取消、重试与失败
传播、任务级/运行级超时，以及历史运行的过滤分页与汇总统计。

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
| POST | `/pipelines/{name}/runs` | 可带 `timeout_seconds` 给整个运行设最长执行时间 |
| GET | `/runs?state=&pipeline=&created_from=&created_to=&limit=&offset=` | 过滤分页查询历史运行 |
| GET | `/runs/stats?pipeline=&created_from=&created_to=` | 汇总统计（各状态数、排队数、平均耗时） |
| GET | `/runs/{run_id}` | 查询运行状态（含每个任务的尝试记录） |
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

## 并发容量与排队

- 全局只有一个并发上限（默认 8 个运行同时 RUNNING），可用环境变量
  `MAX_CONCURRENT_RUNS` 调整；任务执行线程数由 `MAX_TASK_WORKERS` 调整
  （默认 64），驱动线程与任务线程分属两个线程池，批量触发不会把调度
  线程饿死。
- 触发后运行先进入 `QUEUED`：超容量的运行在队列里等待，不启动任何任务，
  与 `RUNNING` 在状态字段上明确可区分。
- 容量空出后严格按触发先后（FIFO，内部单调序号）放行，不会有运行一直
  排不上。
- 排队中的运行可以直接取消：立即进入 `CANCELLED`（`reason` 为
  `cancelled while queued`），所有任务保持 0 次尝试、标记 SKIPPED；
  此后绝不会被调度启动。对终态运行重复取消返回 `409`。
- 排队不改变幂等语义：相同 `(流程名, idempotency_key)` 的并发/迟到触发
  仍然只返回同一个运行（即使它还在 QUEUED）。

## 超时规则

- 任务级超时：在任务定义里给任务配 `timeout_seconds`（正数秒）。某一次
  尝试超时后，该次尝试记 FAILED（错误信息含 `timed out`），按任务现有
  的重试规则继续；重试用尽后任务 FAILED，走既有的失败传播与下游 SKIPPED
  链路。超时的尝试绝不会记成成功，即使执行体稍后才真正返回。
- 运行级超时：发起运行时传 `timeout_seconds`。运行从拿到并发槽
  （RUNNING）开始计时；超时后未开始的任务一律不再启动（SKIPPED），
  正在执行的任务通过取消/超时事件尽快收敛到 CANCELLED/FAILED，运行最终
  为 FAILED，`reason` 明确写出 `run timed out after Ns`，
  `timeout_seconds` 原样回显。
- 排队等待时间不计入运行级超时。
- 未配置任何超时时行为与旧版完全一致（定义里没有 `timeout_seconds`、
  触发时不传该参数即可）。

## 历史查询与统计

- `GET /runs` 支持 `pipeline`（流程名）、`state`
  （`QUEUED/RUNNING/SUCCEEDED/FAILED/CANCELLED`）、`created_from` /
  `created_to`（Unix 秒，闭区间）过滤，以及 `limit`（默认 50，最大 500）
  和 `offset` 分页。响应含 `total/limit/offset`，结果按触发顺序稳定排列，
  翻页不重不漏。
- `GET /runs/stats` 返回 `total`、各状态计数 `by_state`、当前
  `queued` 数、并发槽占用 `running_slots_active/running_slots_capacity`、
  `completed` 与已完成运行的 `average_duration_seconds`（平均耗时，
  QUEUED 等待不计入耗时；没有已完成运行时为 `null`）。两个接口支持同样
  的流程名/时间范围参数，统计口径与列表明细一致。
- 查询和统计只在短锁内复制一份运行快照，过滤与聚合在锁外完成，不阻塞
  正在进行的触发与调度。

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

- 运行状态：`QUEUED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED`，后三个
  为终态。QUEUED 可以直接转到 CANCELLED（排队取消，无任何任务启动）。
- 仅当所有任务都进入终态后，运行才会进入终态；接口不会返回“运行已失败
  但仍有任务 RUNNING”这类自相矛盾的结果。
- 任一任务 FAILED 则运行 FAILED；其传递闭包下游标记 SKIPPED（`error`
  给出阻塞来源），SKIPPED 任务尝试次数为 0，从不执行也不计入成功。
- 与失败任务无依赖关系的分支继续执行，不受影响。
- 取消：未开始的任务立即 SKIPPED 且永不再被调度；进行中的任务通过
  cancel 事件中断并收敛到 CANCELLED；`/cancel` 等待全部任务收敛后再返回。
  已处于终态的运行再次取消返回 `409 RUN_NOT_CANCELLABLE`。
- 超时：任务级超时只让当前尝试失败并进入重试；运行级超时按取消式收敛
  处理，运行终态为 FAILED 并带明确 `reason`。
- 每次运行响应额外包含 `reason`（终态原因）、`started_at`、
  `timeout_seconds`、`duration_seconds`；原有字段保持不变。

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
- 流程名提交时自动去除首尾空格：`"  etl  "` 与 `"etl"` 是同一个流程，
  版本号在此名字下连续唯一；重复提交相同定义（即使中间提交过其他版本）
  总是复用最早匹配的那个版本。任务的 `timeout_seconds` 也属于定义差异，
  变化会产生新版本。
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
uv run pytest tests/test_concurrency.py   # 批量并发/排队/排队取消
uv run pytest tests/test_timeouts.py      # 任务级/运行级超时
uv run pytest tests/test_history.py       # 过滤分页与统计一致性
uv run pytest tests/test_submission_boundaries.py
```

手工复现批量并发与排队（把容量调小更容易观察）：

```bash
MAX_CONCURRENT_RUNS=3 uv run uvicorn main:app --reload
curl -s -X POST localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"demo",
  "tasks":[{"name":"a","dependencies":[],
            "action":{"type":"sleep","seconds":2}}]}'
for i in $(seq 1 10); do
  curl -s -X POST localhost:8000/pipelines/demo/runs -H 'content-type: application/json' -d '{}'
done
curl -s localhost:8000/runs/stats          # 看到 queued=7、3 个槽位占用
curl -s "localhost:8000/runs?state=QUEUED" # 排队中的运行
```

验证超时：定义里给任务加 `"timeout_seconds": 0.2`（任务配长 sleep 可观察
重试与失败传播）；触发时传
`-d '{"timeout_seconds": 1}'` 验证运行级超时与 `reason`。

验证查询统计：`curl "localhost:8000/runs?pipeline=demo&state=SUCCEEDED&limit=10&offset=0"`，
再对照 `/runs/stats` 中各状态计数与平均耗时。

测试日志包含两类业务可读信息：

- 每个用例开始时输出 `[业务覆盖] 业务=<分组> 用例=<名称> 开始执行`，
  运行过程中编排器输出创建、尝试、失败、跳过、取消等事件日志；
- 结束时输出“业务覆盖汇总”，按业务分组列出用例及通过/失败情况，
  覆盖：非法定义（空/重名/自依赖/缺依赖/成环/非法动作/错误可区分）、
  定义版本化、调度顺序、重复与并发幂等、同流程并发运行、取消、
  重试记录、失败传播、成功任务不重复记账、跨版本历史结论稳定性、
  批量并发收敛与压后恢复、FIFO 排队可见性与不饿死、排队取消零任务启动、
  排队幂等、任务级/运行级超时（重试、传播、不重复记账）、历史过滤分页、
  统计与明细一致性、查询性能、名称空格归一化与版本连续/复用边界。
