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
- **运行级超时**：触发时传 `run_timeout_seconds`。超时后运行立即判为
  `FAILED`，`failure_reason` 为 `run exceeded timeout of Xs`：尚未开始的
  任务不再启动并标记 `SKIPPED`，正在执行的任务被中断并收敛为 `FAILED`。
  **收敛期间运行对外仍显示 `RUNNING`**，直到全部在途任务都进入终态后，
  运行才一次性发布 `FAILED` 终态；因此等待接口、运行详情、运行列表、
  统计等任何读接口，在任何时刻都不会看到“运行已失败但任务还在
  RUNNING/PENDING”的自相矛盾记录，同一份运行反复查询结论一致。被中断
  的在途尝试记为该次尝试 `FAILED`，**不记成功、不消耗重试预算、不补发
  新尝试**，其下游也不会被解锁启动。
- **取消与超时的区分**：取消收敛为 `CANCELLED`，在途任务收敛到
  `CANCELLED`，`failure_reason` 为 `run cancelled`（排队中取消为
  `cancelled while queued: no task was started`）；整体超时收敛为
  `FAILED`，在途任务收敛到 `FAILED`，`failure_reason` 含
  `exceeded timeout`。两类终态与原因始终可区分；先到的中止原因生效，
  超时与取消不会互相覆盖。
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
- 仅当所有任务都进入终态后，运行才会进入终态；接口不会返回“运行已失败
  但仍有任务 RUNNING”这类自相矛盾的结果。
- 任一任务 FAILED 则运行 FAILED；其传递闭包下游标记 SKIPPED（`error`
  给出阻塞来源），SKIPPED 任务尝试次数为 0，从不执行也不计入成功。
- 与失败任务无依赖关系的分支继续执行，不受影响。
- 取消：未开始的任务立即 SKIPPED 且永不再被调度；进行中的任务通过
  cancel 事件中断并收敛到 CANCELLED；`/cancel` 等待全部任务收敛后再返回。
  已处于终态的运行再次取消返回 `409 RUN_NOT_CANCELLABLE`。
- 排队中取消：不经过 RUNNING，直接 `QUEUED -> CANCELLED`，任务全部
  SKIPPED、尝试次数为 0。
- 运行整体超时：先在内部挂起失败结论并中断在途任务（对外仍为
  `RUNNING`），待全部任务收敛后一次性发布 `RUNNING -> FAILED`，原因
  写入 `failure_reason`；在途任务收敛为 FAILED（当次尝试 FAILED、不
  重试、不记成功），未开始任务 SKIPPED。任何读接口都观察不到“运行已
  失败而任务仍在 RUNNING/PENDING”的中间状态。取消与超时的终态
  （CANCELLED vs FAILED）及原因始终可区分。

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

- 定义发布后不可修改：提交时会把新定义与该流程**历史上的每一个版本**
  逐一比对，只要任务、依赖、动作、重试配置、任务/默认超时配置与任一
  历史版本完全相同，就复用那个版本号并返回 `reused=true`——**包括回退
  重发更早的历史版本**（例如已发布 v1、v2、v3 后，把 v1 的内容原样
  重发，得到的仍是 v1，不会生出内容重复的 v4）；只有与所有历史版本
  都不同时才生成递增新版本。
- 流程名提交时自动去除前后空格：`"etl"`、`" etl "` 视为同一个流程；
  版本号始终连续唯一（1, 2, 3 ...），相同定义（含带空格的名字）重复
  提交仍复用原版本号。
- 多人/多线程同时提交同一份定义时，比对与入库在同一个锁内原子完成，
  只会产生一个版本：所有并发提交都拿到同一个版本号，且除入库的那次
  外均返回 `reused=true`。
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
               tests/test_history_stats.py tests/test_submission_boundaries.py \
               tests/test_version_reuse_timeout_convergence.py
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
  {"name":"b","dependencies":[],"action":{"type":"sleep","seconds":5}},
  {"name":"c","dependencies":["a","b"],"action":{"type":"succeed"}}]}'
curl -s -X POST localhost:8000/pipelines/rt/runs -H 'content-type: application/json' \
  -d '{"run_timeout_seconds":0.2}'
# 超时前后高频轮询：收敛期间只可能看到 RUNNING（任务可能在跑），
# 一旦 FAILED，所有任务必然已是 FAILED/SKIPPED，不会出现矛盾记录
RID=<run_id>
for i in $(seq 1 20); do curl -s localhost:8000/runs/$RID; sleep 0.05; done

# 回退重发历史版本：先发 v1、v2，再把 v1 原样（名字可带空格）重发，
# 返回 version=1、reused=true，版本列表仍只有 1、2
curl -s localhost:8000/pipelines -H 'content-type: application/json' \
  -d '{"name":"etl","tasks":[{"name":"a","dependencies":[],"action":{"type":"succeed"}}]}'
curl -s localhost:8000/pipelines -H 'content-type: application/json' \
  -d '{"name":"etl","tasks":[{"name":"a","dependencies":[],"action":{"type":"fail"}}]}'
curl -s localhost:8000/pipelines -H 'content-type: application/json' \
  -d '{"name":"  etl  ","tasks":[{"name":"a","dependencies":[],"action":{"type":"succeed"}}]}'
curl -s localhost:8000/pipelines/etl/versions
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
  不记成功）、运行级超时（未开始不启动、在途收敛、原因明确）、多在途
  任务超时收敛时详情/列表/等待各读接口全程一致无矛盾、超时不补发
  重试不解锁下游、取消与超时终态原因可区分、排队取消不启动任务、
  无超时兼容旧行为、提交名称空白归一、版本连续唯一、相同定义复用
  版本（跨版本回退重发、重试/超时配置参与比对、并发重复提交只得
  一个版本）、历史过滤分页不重不漏、统计与明细对账、平均耗时、
  上百运行查询性能。
