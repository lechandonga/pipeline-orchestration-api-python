# 本地数据管道编排服务

纯本地运行的流程编排 API：用内存存储 + 模拟任务执行器代替真实外部系统，
支持流程定义版本化、并发运行（容量准入 + FIFO 排队）、幂等触发、取消、
重试、任务/运行级超时、失败传播，以及可过滤分页的历史查询与汇总统计。

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
| GET | `/runs` | 列出/过滤/分页运行（见“历史查询与统计”） |
| GET | `/runs-stats` | 汇总统计：各状态运行数、排队数、平均耗时 |
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
       "action": {"type": "flaky", "succeed_on_attempt": 2},
       "max_retries": 3, "timeout_seconds": 10},
      {"name": "load", "dependencies": ["transform"], "action": {"type": "succeed"}}
    ]
  }
  ```

模拟动作：`succeed`（立即成功）、`fail`（每次必失败，带 `message`）、
`flaky`（第 `succeed_on_attempt` 次尝试起成功）、`sleep`（休眠 `seconds`
后成功，取消/超时时可被即时中断）。

发起运行时还可以限制整个运行的最长执行时间（从运行被准入、真正开始
执行的时刻算起，排队等待时间不计入）：

```json
{"run_timeout_seconds": 60}
```

## 容量与排队

- 服务有一个**整体并发容量**（同时处于 `RUNNING` 的运行上限），默认 8，
  可用环境变量 `PIPELINE_MAX_CONCURRENT_RUNS` 调整（例如 CI 启动前
  `export PIPELINE_MAX_CONCURRENT_RUNS=16`）。
- 触发后运行先进 `QUEUED` 状态；容量空闲时由准入线程按 **FIFO（触发先后）**
  顺序准入，先排队的先执行，不会被后来的运行饿死。
- `QUEUED` 与 `RUNNING` 在 `GET /runs/{id}` 与列表中明确区分。
- 排队中的运行可以取消：立即进入 `CANCELLED`，`failure_reason` 为
  `cancelled while queued: no task was started`，全部任务保持
  `PENDING -> SKIPPED`、尝试次数为 0，**一个任务都不会启动**，运行也不再
  占用容量；队列中其余运行照常推进。
- 正在运行的运行取消语义与旧版一致（未开始任务 SKIPPED，在途任务收敛
  到 CANCELLED）。
- 容量只按“运行”计数，不限制单个运行内部任务并行度；任务线程池大小按
  容量自动配置。

排队场景下的幂等与版本：

- 幂等键在**创建（入队）时**就绑定：运行处于 `QUEUED` 时重复触发相同
  `(流程名, key)` 仍返回同一个 `run_id`（`created=false`）；排队中被取消
  后再用同一 key 触发，回放的仍是那个已取消运行，不会新建。
- 版本快照同样在入队时固化，排队期间流程发布了新版本不影响该运行。

## 超时规则

- **任务级超时**：任务定义支持 `timeout_seconds`（正数秒）；流程级可用
  `default_task_timeout_seconds` 给所有任务兜底，任务级覆盖流程级。
  超时按**每次尝试**单独计时：一次尝试超时记为该次尝试 `FAILED`
  （attempt 记录 `error` 含 `timed out`），随后按既有重试规则继续；
  重试用尽则任务 `FAILED`，错误信息为
  `timed out after N attempt(s): ...`，下游照旧 `SKIPPED`。
  超时绝不会被记成成功。
- **运行级超时**：触发时传 `run_timeout_seconds`。超时时刻调度器立即
  停发新任务（不再发起任何新尝试，也不解锁任何下游），尚未开始的
  任务标记 `SKIPPED`，正在执行的任务被中断、收敛为 `FAILED`；
  **在途任务全部收敛后**运行才进入 `FAILED` 终态，`failure_reason` 为
  `run exceeded timeout of Xs`。收敛过程中读接口看到的仍是
  `RUNNING`，因此 `GET /runs/{id}`、`GET /runs`、`/wait` 在任何时刻都
  不会返回“运行已 FAILED 但还有任务 RUNNING/PENDING”的矛盾记录，同一
  份运行前后两次查询只会从 `RUNNING` 变成自洽的 `FAILED`。
- **取消与超时的区分**：取消终态为 `CANCELLED`，在途任务收敛到
  `CANCELLED`，原因含 `cancel`；超时终态为 `FAILED`，在途任务收敛到
  `FAILED`，原因含 `timeout`。排队中取消仍为直接
  `QUEUED -> CANCELLED`，全部任务 SKIPPED、尝试次数为 0，一个任务都
  不会启动。
- **未配置任何超时的流程行为与旧版完全一致**：没有截止时间，动作不被
  提前打断。

## 状态流转规则

任务状态：

```
PENDING ──> RUNNING ──> SUCCEEDED
   │          │
   │          └──> CANCELLED        （取消时正在执行，被中断/随取消收敛）
   │          └──> FAILED           （运行整体超时时在途任务被中断）
   ├──> FAILED                      （重试上限耗尽，含任务超时耗尽）
   └──> SKIPPED                     （上游 FAILED/SKIPPED/CANCELLED，
                                     运行取消/整体超时尚未开始，
                                     或排队中被取消）
```

- 运行状态：`QUEUED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED`，后三个
  为终态；`QUEUED` 表示已触发、等待容量，与 `RUNNING` 明确区分。
- 仅当所有任务都进入终态后，运行才会进入终态；运行整体超时/取消时，
  调度器先标记“待收敛”、停发新任务并收敛在途任务，全部任务终态化后才
  发布运行终态。收敛期间对外仍是 `RUNNING`，因此等待、详情、列表任何
  读接口都不会返回“运行已失败/已取消但仍有任务 RUNNING”的矛盾结果。
- 任一任务 FAILED 则运行 FAILED；其传递闭包下游标记 SKIPPED（`error`
  给出阻塞来源），SKIPPED 任务尝试次数为 0，从不执行也不计入成功。
- 与失败任务无依赖关系的分支继续执行，不受影响。
- 取消：未开始的任务立即 SKIPPED 且永不再被调度；进行中的任务通过
  cancel 事件中断并收敛到 CANCELLED；`/cancel` 等待全部任务收敛后再返回。
  已处于终态的运行再次取消返回 `409 RUN_NOT_CANCELLABLE`。
- 排队中取消：不经过 RUNNING，直接 `QUEUED -> CANCELLED`，任务全部
  SKIPPED、尝试次数为 0。
- 运行整体超时：收敛后 `RUNNING -> FAILED`，`failure_reason` 为
  `run exceeded timeout of Xs`；在途任务收敛为 FAILED（即使动作在超时
  后才返回成功，该次尝试也记 FAILED，绝不记成功），未开始任务 SKIPPED；
  超时后不再发起新尝试、不解锁下游。与取消（`CANCELLED`）终态和原因
  明确可区分。

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

## 历史查询与统计

`GET /runs` 支持以下查询参数（全部可选，可组合）：

| 参数 | 说明 |
| --- | --- |
| `pipeline` | 按流程名精确过滤（自动忽略名称前后空格） |
| `state` | 按运行状态过滤：`QUEUED/RUNNING/SUCCEEDED/FAILED/CANCELLED`，逗号分隔可多选 |
| `start_time` / `end_time` | 按**触发时间**（Unix 秒）做闭区间过滤 |
| `limit` / `offset` | 分页，`limit` 默认 50、上限 500，`offset` 默认 0 |

响应：

```json
{"runs": [ ... ], "total": 137, "limit": 50, "offset": 0}
```

- 结果按触发顺序（内部单调序号）返回，翻页稳定，**不重不漏**；过滤、
  计数、切片基于同一份内存快照完成。
- 运行对象新增只读字段：`queued_at`（墙钟触发时间）、`wall_started_at`、
  `wall_finished_at`、`started_at`、`timeout_seconds`、`failure_reason`；
  原有字段（`run_id`/`pipeline`/`version`/`state`/`created_at`/
  `finished_at`/`tasks` 等）保持不变。
- 查询在编排锁内只做轻量筛选，序列化使用运行的分离快照，不会阻塞正在
  进行的触发与调度。

`GET /runs-stats`（可选 `pipeline`、`start_time`、`end_time`，语义同上）：

```json
{
  "states": {"QUEUED": 0, "RUNNING": 1, "SUCCEEDED": 130,
             "FAILED": 5, "CANCELLED": 1},
  "total": 137,
  "queued": 0,
  "running": 1,
  "finished": 136,
  "average_duration_seconds": 0.042
}
```

- `average_duration_seconds` 为过滤范围内所有已完成（终态）运行的
  `wall_finished_at - wall_started_at` 平均值；没有已完成运行时为 `null`。
- 统计与 `/runs` 明细使用同一套过滤口径，各状态计数之和等于 `total`，
  与带 `state=` 过滤查出的条数一致。

## 版本兼容范围

- 定义发布后不可修改：重复提交**与历史上任何一个版本完全相同**的定义
  都返回那个已有版本（`reused=true`），而不只是与最近一版比较。典型的
  回滚场景——先发 v1、再发 v2、然后把 v1 内容原样重发——会直接复用
  v1，不会凭空产生内容重复的 v3；v1/v2/v1/v2 交替重发也始终只有两个
  版本。判定相同的字段包括任务集合、依赖、动作、任务/流程级重试配置、
  任务/流程级超时配置；任一字段变化才生成递增新版本。
- 版本号始终连续唯一（1, 2, 3 ...）；多人/多线程并发提交同一份定义
  （含并发回退重发）时，注册中心在同一把锁内完成“查重—创建”，只有一个
  请求创建版本，其余全部得到同一个版本号且 `reused=true`。
- 流程名提交时自动去除前后空格：`"etl"`、`" etl "` 视为同一个流程；
  版本号始终连续唯一（1, 2, 3 ...），相同定义（含带空格的名字）重复
  提交仍复用原版本号。
- 每个运行在创建时固化定义快照（响应中的 `version`），历史运行永远按
  发起时的版本解释与展示；之后提交新版本不改变旧运行的任何结论。
- 发起运行时可用 `version` 显式指定历史版本；不指定则使用最新版本。

其他兼容性约定：

- **不配任何超时**时，行为与旧版完全一致：运行不设整体截止时间，任务
  动作不被超时打断。
- 原有响应字段全部保留，新字段为纯增量；`GET /runs` 的响应由
  `{"runs": [...]}` 扩展为带 `total/limit/offset` 的分页对象，`runs`
  字段本身不变。
- 运行新增 `QUEUED` 状态；旧客户端若按“非 RUNNING 即终态”判断，需要
  改认 `SUCCEEDED/FAILED/CANCELLED` 为终态（`/wait` 只在终态返回）。
- 排队不影响幂等：幂等键在入队时绑定，QUEUED 期间与取消后重复触发都
  回放同一运行；版本同样在入队时固化。

非法定义的错误码（HTTP 422，`detail.code`）：`EMPTY_DEFINITION`、
`DUPLICATE_TASK`、`SELF_DEPENDENCY`、`MISSING_DEPENDENCY`、
`CYCLIC_DEPENDENCY`、`INVALID_ACTION`。资源不存在为 404
（`PIPELINE_NOT_FOUND` / `RUN_NOT_FOUND`）。

## 本地验证

```bash
uv run pytest                 # 运行全部单测
uv run pytest -q              # 安静模式，末尾打印“业务覆盖汇总”
uv run pytest -k 取消          # 只跑取消相关业务
uv run pytest tests/test_capacity_queue.py tests/test_timeouts.py \
               tests/test_history_stats.py tests/test_submission_boundaries.py
# 版本复用（跨版本回退/名字空白/并发提交）与运行超时收敛
uv run pytest tests/test_version_reuse.py tests/test_run_timeout_convergence.py -v
```

验证跨版本回退复用（先发 v1、改后发 v2、原样重发 v1 应复用版本 1）：

```bash
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"rb","default_max_retries":1,
  "tasks":[{"name":"a","dependencies":[],
  "action":{"type":"fail","message":"v1"},"max_retries":2,
  "timeout_seconds":1.0},
  {"name":"b","dependencies":["a"],"action":{"type":"succeed"}}]}'
# 发布 v2（任意改动，例如动作变为 succeed）
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"rb","default_max_retries":1,
  "tasks":[{"name":"a","dependencies":[],
  "action":{"type":"succeed"},"max_retries":2,
  "timeout_seconds":1.0},
  {"name":"b","dependencies":["a"],"action":{"type":"succeed"}}]}'
# 原样重发 v1（名字前后多敲空格也一样）：期望 "version":1,"reused":true
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"  rb  ","default_max_retries":1,
  "tasks":[{"name":"a","dependencies":[],
  "action":{"type":"fail","message":"v1"},"max_retries":2,
  "timeout_seconds":1.0},
  {"name":"b","dependencies":["a"],"action":{"type":"succeed"}}]}'
curl -s localhost:8000/pipelines/rb/versions   # 仍只有 1、2 两个版本
```

手工复现 CI 批量并发与排队（默认容量 8）：

```bash
uv run uvicorn main:app --port 8000
# 1) 提交一个短任务流程
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"ci-job","tasks":[{"name":"a","dependencies":[],
  "action":{"type":"sleep","seconds":1}}]}'
# 2) 一口气触发 40 个
for i in $(seq 40); do
  curl -s -X POST localhost:8000/pipelines/ci-job/runs -H 'content-type: application/json' -d '{}'
done | grep -o '"state":"[A-Z]*"' | sort | uniq -c
# 3) 观察 QUEUED 与 RUNNING 并存
curl -s 'localhost:8000/runs?pipeline=ci-job&limit=100' | python -m json.tool
curl -s 'localhost:8000/runs-stats?pipeline=ci-job'
# 4) 排队取消：取一个 QUEUED 运行 id
curl -s -X POST localhost:8000/runs/<queued_run_id>/cancel
#    返回中 state=CANCELLED、failure_reason 为 cancelled while queued，
#    所有任务 SKIPPED 且 attempt_count=0
```

验证任务级与运行级超时：

```bash
# 任务超时：每次尝试 0.05s 上限，动作要跑 1s，重试 1 次 -> 2 次 FAILED 后任务 FAILED
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"tt","tasks":[{"name":"a","dependencies":[],
  "timeout_seconds":0.05,"max_retries":1,
  "action":{"type":"sleep","seconds":1}}]}'
curl -s -X POST localhost:8000/pipelines/tt/runs -H 'content-type: application/json' \
  -d '{"wait":true,"wait_timeout":10}'

# 运行整体超时：0.2s 后运行 FAILED，在途任务收敛、下游 SKIPPED
curl -s localhost:8000/pipelines -H 'content-type: application/json' -d '{
  "name":"rt","tasks":[{"name":"a","dependencies":[],
  "action":{"type":"sleep","seconds":5}},
  {"name":"b","dependencies":["a"],"action":{"type":"succeed"}}]}'
curl -s -X POST localhost:8000/pipelines/rt/runs -H 'content-type: application/json' \
  -d '{"run_timeout_seconds":0.2,"wait":true,"wait_timeout":10}'
```

验证查询与统计：

```bash
curl -s 'localhost:8000/runs?pipeline=ci-job&state=SUCCEEDED&limit=20&offset=0'
curl -s 'localhost:8000/runs?state=FAILED,CANCELLED'
curl -s "localhost:8000/runs?start_time=$(date +%s)&end_time=$(date +%s)"
curl -s 'localhost:8000/runs-stats'
# 对账：各状态 ?state= 条数之和应等于 stats.total
```

测试日志包含两类业务可读信息：

- 每个用例开始时输出 `[业务覆盖] 业务=<分组> 用例=<名称> 开始执行`，
  运行过程中编排器输出创建、尝试、失败、跳过、取消等事件日志；
- 结束时输出“业务覆盖汇总”，按业务分组列出用例及通过/失败情况，
  覆盖：非法定义（空/重名/自依赖/缺依赖/成环/非法动作/错误可区分）、
  定义版本化、调度顺序、重复与并发幂等、同流程并发运行、取消、
  重试记录、失败传播、成功任务不重复记账、跨版本历史结论稳定性、
  批量并发收敛（40 个运行）、容量排队可见性、FIFO 不饿死、排队取消
  不启动任务、压测后可接新触发、排队幂等、任务级超时（重试/失败传播/
  不记成功）、运行级超时（未开始不启动、在途收敛、原因明确）、无超时
  兼容旧行为、提交名称空白归一、版本连续唯一、相同定义复用版本
  （含跨版本回退重发复用最早版本、并发重复提交只产生一个版本）、
  运行超时时多个在途任务最终收敛、等待/详情/列表及并发读接口全程
  状态自洽、超时与取消终态和原因可区分、超时后在途任务绝不记成功、
  历史过滤分页不重不漏、统计与明细对账、平均耗时、上百运行查询性能。
