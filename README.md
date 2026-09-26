# 分布式追踪尾部采样决策 API

Python · FastAPI · PostgreSQL · SQLAlchemy (async)

客户端按 `trace_id / span_id / parent_id / links` 上报**可能乱序**的 span；服务在
**空闲窗口关闭**、**显式结束**或**越过硬水位线**时重建完整 trace，并依据**带版本的
采样策略**（错误、延迟、属性组合、父子/links 传播规则）决定 `RETAIN` / `DISCARD` /
`INDETERMINATE`，同时生成可解释的证据（evidence）。

---

## 1. 核心语义

| 主题 | 语义 |
| --- | --- |
| 乱序上报 | span 可任意顺序、分多批到达；收尾时按 `parent_id` 重建树（排序、检测环与孤儿 span） |
| 收尾时机 | ① 显式 `POST /end`；② 空闲窗口（距最后一个 span 超过 `TS_WINDOW_MS`）；③ 硬水位线（距当前 revision 首个 span 超过 `TS_HARD_WATERMARK_MS` 仍未闭合 → `INDETERMINATE`） |
| 迟到 span | 决策发布后到达的 span **触发新 revision**（revision+1，trace 重新打开）；**永不改写**已发布的决定 |
| 不确定 | 未闭合 trace 越过硬水位线，归档为 `INDETERMINATE`；证据里附带 `would_have_verdict`（规则“本会”给出的结论，仅供参考） |
| 策略发布 | 全程 CAS：`expected_version` 必须等于当前活跃版本；冲突返回 `409 CAS_CONFLICT`（事务级 advisory lock 串行化发布者） |
| 决策落库 | 决策与证据在**同一事务**原子写入；`(trace_id, revision)` 唯一约束 + trace 行锁，并发收尾**绝不会重复归档** |
| 幂等 | `Idempotency-Key` 请求头：同键**同参重放**原响应（含完全相同的状态码与响应体，重放响应带 `X-Idempotent-Replay: true`）；同键**异参冲突** `409 IDEMPOTENCY_CONFLICT`。并发首请求采用**两阶段协议**：①“占位事务”先用 `INSERT ... ON CONFLICT DO NOTHING` 原子写入**立即提交**的 PENDING 占位（含请求指纹与 run_token），因此处理中占位对其他事务**立即可见**；②“业务事务”对占位行 `SELECT … FOR UPDATE` 加行锁并运行业务，成功时同事务翻转为 COMPLETED 保存原状态码/响应体。于是：异参请求**不等待**业务完成——读到处理中占位即返回 409（业务绝不执行）；同参请求在行锁上等待，首请求提交后拿到**完全相同的状态码与响应体**；业务只执行一次。业务失败时通过 SAVEPOINT 回滚业务写入、在同一事务把占位标记为 FAILED 再提交，等待的同参请求自动重选一位接管**受控重试**（不会出现唯一约束 500）；所有重试都失败后键立即可被修正后的请求复用。硬崩溃留下的 PENDING 按 TTL 接管（`attempts` 计数）；**接管探测在 `FOR UPDATE` 行锁保护下重读权威行**（`populate_existing`，不沿用 ORM 会话缓存的旧对象）——若首写业务超过 TTL 后在探测前提交，已完成行**保留首写指纹、状态码与响应体**：同参重放、异参立即 409，只有在锁保护下仍确为过期 PENDING 的占位才会被接管 |
| span 幂等 | 同 `span_id` 同内容 → `duplicate`（200）；同 `span_id` 不同内容 → `409 SPAN_CONFLICT`（整批不落库） |
| 重放 | 可对任意历史 revision、用任意策略版本做**只读**重算，返回结论及与已发布结论的差异 |

默认窗口 30s、硬水位线 60s（可用环境变量调整，见下）。

## 2. 一键启动

```bash
docker compose up --build
```

- API: http://localhost:8080
- Swagger UI: http://localhost:8080/docs
- 健康检查: http://localhost:8080/health ，就绪检查: http://localhost:8080/ready
- PostgreSQL: localhost:5432（用户/库：`tail/tailsampling`）

首次启动会自动建表并发布内置策略 **v1**（含“任意错误 → 保留”“慢 span(>1s) → 保留”，
默认丢弃）。

> 访问方式：API 基址 `http://localhost:8080`，交互文档 `http://localhost:8080/docs`，
> 健康/就绪探针 `http://localhost:8080/health`、`http://localhost:8080/ready`。

### 不用 Docker 时（本地 Python + 已有 PostgreSQL）

```bash
pip install -r requirements.txt
export TS_DATABASE_URL='postgresql+asyncpg://tail:tail@localhost:5432/tailsampling'
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

启动同样会自动建表/补列（`idempotency_keys.run_token` 等）并播种 v1 策略。

### 配置（环境变量，`TS_` 前缀）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `TS_DATABASE_URL` | `postgresql+asyncpg://tail:tail@db:5432/tailsampling` | 异步 PG 连接串 |
| `TS_WINDOW_MS` | 30000 | 空闲窗口（毫秒） |
| `TS_HARD_WATERMARK_MS` | 60000 | 硬水位线（毫秒） |
| `TS_SWEEP_INTERVAL_MS` | 1000 | 后台扫描周期 |
| `TS_ENABLE_SWEEPER` | true | 关闭后只靠显式 `/end` 与 `POST /admin/sweep` 收尾 |
| `TS_SEED_DEFAULT_POLICY` | true | 首次启动播种内置 v1 策略 |
| `TS_IDEMPOTENCY_PENDING_TTL_MS` | 60000 | 处理中 PENDING 占位多久无活跃属主（行锁可获取）后可被接管；仅用于进程硬崩溃/连接丢失恢复，正常失败会立即转为 FAILED 并由等待者接管 |
| `TS_ENABLE_TEST_HOOKS` | false | 仅供测试的 `/admin/test/*` 故障注入端点（放慢业务事务、统计执行次数）；镜像与 compose 不启用 |

## 3. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/traces/{trace_id}/spans` | 上报一批 span（支持 `Idempotency-Key`） |
| POST | `/v1/traces/{trace_id}/end` | 显式结束并立即决策（并发安全、可重放） |
| GET | `/v1/traces/{trace_id}` | 查询 trace 及全部 span |
| GET | `/v1/traces/{trace_id}/decision?revision=n` | 决策 + 证据（解释） |
| GET | `/v1/traces/{trace_id}/revisions` | 全部 revision 的决策列表 |
| POST | `/v1/traces/{trace_id}/revisions/{n}/replay?policy_version=v` | 只读重放某 revision |
| GET | `/v1/policies` / `/active` / `/{version}` | 策略列表 / 活跃 / 指定版本 |
| POST | `/v1/policies` | CAS 发布新策略版本 |
| POST | `/admin/sweep` | 立即跑一次窗口/水位线扫描（演示/测试用） |

## 4. 快速示例

以下用 `curl` 演示一个完整生命周期（时间用 ISO-8601，也可只给 `start_time`+`duration_ms`）。

### 4.1 上报乱序 span（先报子 span，后报父 span）—— 错误 trace 被保留

```bash
TID=demo-error

# 先到的是叶子 span（ERROR），此时父 span 还没到
curl -s -X POST "localhost:8080/v1/traces/$TID/spans" \
  -H 'Content-Type: application/json' \
  -d '{"spans":[{
    "span_id":"span-leaf", "parent_id":"span-root",
    "name":"charge", "status":"ERROR",
    "start_time":"2026-09-21T10:00:00.100Z", "duration_ms":250,
    "attributes":{"service":"payments","error":"card_declined"}
  }]}'

# 后到父 span（乱序）
curl -s -X POST "localhost:8080/v1/traces/$TID/spans" \
  -H 'Content-Type: application/json' \
  -d '{"spans":[{
    "span_id":"span-root", "parent_id":null,
    "name":"POST /checkout", "status":"OK",
    "start_time":"2026-09-21T10:00:00.000Z", "duration_ms":300,
    "attributes":{"service":"gateway","http.route":"/checkout"}
  }]}'

# 显式结束并决策
curl -s -X POST "localhost:8080/v1/traces/$TID/end" | python3 -m json.tool
```

返回中可见 `verdict: "RETAIN"`，证据说明错误从叶子沿父子链**向上传播**：

```jsonc
{
  "trace": {"state": "ARCHIVED", "revision": 1, ...},
  "decision": {
    "verdict": "RETAIN",
    "close_reason": "END_SIGNAL",
    "evidence": {
      "matched_rule": {"rule_id": "error-any", "matched_span_ids": ["span-leaf", "span-root"], ...},
      "effective_error_span_ids": ["span-leaf", "span-root"],
      "error_sources": {"span-root": ["child_error:span-leaf"]},
      "explanation": "RETAIN: rule 'error-any' matched spans ['span-leaf', 'span-root'] under scope 'any'"
    }
  }
}
```

### 4.2 迟到 span 触发新 revision，旧决定不被改写

```bash
# 上面的 trace 已归档。一个迟到的、指向其他错误 span 的 link 到达：
curl -s -X POST "localhost:8080/v1/traces/$TID/spans" \
  -H 'Content-Type: application/json' \
  -d '{"spans":[{
    "span_id":"span-late-link", "parent_id":"span-root",
    "name":"async-followup", "status":"OK",
    "start_time":"2026-09-21T10:00:00.400Z", "duration_ms":20,
    "links":[{"trace_id":"other-trace","span_id":"span-x","is_error":true,
              "attributes":{"link.type":"follows_from"}}]
  }]}'
# => "revision": 2, "state": "OPEN", "note": "trace reopened as new revision"

# revision 1 的历史决定原封不动
curl -s "localhost:8080/v1/traces/$TID/decision?revision=1"
# 关闭 revision 2
curl -s -X POST "localhost:8080/v1/traces/$TID/end"
# 全部 revision
curl -s "localhost:8080/v1/traces/$TID/revisions"
```

### 4.3 延迟 / 属性组合 / 父子组合规则 + CAS 策略发布

先取当前活跃版本：

```bash
ACTIVE=$(curl -s localhost:8080/v1/policies/active | python3 -c 'import sys,json;print(json.load(sys.stdin)["version"])')
echo "active version=$ACTIVE"
```

发布一个新策略（CAS 必须带当前版本号；高优先级规则先匹配，
`parent`/`child`/`linked` 谓词表达父子与 links 组合）：

```bash
curl -s -X POST localhost:8080/v1/policies \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\":\"canary-and-slow\",
    \"expected_version\": $ACTIVE,
    \"spec\":{
      \"default_decision\":\"DISCARD\",
      \"propagation\":{\"error_to_parents\":true,\"error_to_children\":false,\"error_link_marks\":\"source\"},
      \"rules\":[
        {\"id\":\"canary-child-error\",\"description\":\"canary 父 span 下出现错误子 span\",
         \"scope\":\"any\",\"priority\":300,\"decision\":\"RETAIN\",
         \"match\":{\"status\":\"ERROR\",
                   \"parent\":{\"attributes\":[{\"key\":\"deployment\",\"op\":\"eq\",\"value\":\"canary\"}]}}},
        {\"id\":\"checkout-retry\",\"description\":\"支付结账且重试>=2\",
         \"scope\":\"any\",\"priority\":200,\"decision\":\"RETAIN\",
         \"match\":{\"attributes\":[
            {\"key\":\"service\",\"op\":\"eq\",\"value\":\"payments\"},
            {\"key\":\"http.route\",\"op\":\"prefix\",\"value\":\"/checkout\"},
            {\"key\":\"retry.count\",\"op\":\"gte\",\"value\":2}]}},
        {\"id\":\"slow\",\"scope\":\"any\",\"priority\":100,\"decision\":\"RETAIN\",
         \"match\":{\"min_duration_ms\":1000}}
      ]
    }
  }" | python3 -m json.tool
```

用**旧版本号**再发一次会得到 `409 CAS_CONFLICT`（响应体含 `current_version`）。

### 4.4 幂等键：同参重放、异参冲突

```bash
curl -s -X POST localhost:8080/v1/traces/idem/spans \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k-1' \
  -d '{"spans":[{"span_id":"a","start_time":"2026-09-21T10:00:00Z","duration_ms":10}]}'
# 同键同参 -> 重放首次结果，不会重复落库
curl -s -X POST localhost:8080/v1/traces/idem/spans \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k-1' \
  -d '{"spans":[{"span_id":"a","start_time":"2026-09-21T10:00:00Z","duration_ms":10}]}'
# 同键异参 -> 409 IDEMPOTENCY_CONFLICT
curl -i -X POST localhost:8080/v1/traces/idem/spans \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k-1' \
  -d '{"spans":[{"span_id":"a","start_time":"2026-09-21T10:00:00Z","duration_ms":999}]}'
```

并发语义（无需客户端额外处理）：多个请求在数据库尚无该键时以**相同键、相同请求体**
同时到达，只有一个请求真正写库；占位行在独立的短事务中**先提交**，业务随后在另一个
事务里持有该占位行的行锁运行，其余同参请求在行锁上阻塞，待首请求提交后拿到
**完全相同的状态码与响应体**（带 `X-Idempotent-Replay: true`）。相同键、**不同**
请求体的并发请求**不会等待业务完成**——处理中的 PENDING 占位已经提交可见，它读到
指纹不同便**立即**返回 `409 IDEMPOTENCY_CONFLICT`（业务不会执行；键完成后的异参
重放同样立即 409）。若首请求业务失败（例如 409 SPAN_CONFLICT 或 5xx），业务写入
经 SAVEPOINT 回滚、占位在同一事务标记为 FAILED 并提交，等待的同参请求自动重选一位
接管**受控重试**——旧实现中“后写者撞唯一约束返回 500”、以及“异参请求被迫等待慢业务
事务结束”这两个问题均已消除；全部尝试失败后该键立即可被修正后的请求复用。
若首写业务超过 `TS_IDEMPOTENCY_PENDING_TTL_MS`，另一请求可能先读到处理中占位、首写随后
才提交：此时接管探测会在 `SELECT … FOR UPDATE SKIP LOCKED` 拿到的**行锁保护下重新读取
权威行**（`populate_existing`，而非沿用同一 ORM 会话里缓存的旧对象）——读到已完成行便
保留首写的指纹、状态码与响应体（同参重放、异参立即 409），只有在锁保护下仍确为
无人持有的过期 PENDING 占位才会被接管（`attempts` 计数），FAILED 行的重选举同样基于
权威行分类，不会沿着缓存快照空转。
`POST /v1/policies` 与 `POST .../end` 这两个写入口使用同一机制。

### 4.5 按 revision 重放（只读，可对比新旧策略）

```bash
# 用策略版本 1 重放 revision 2，看旧策略会怎么判（不影响已发布结论）
curl -s -X POST "localhost:8080/v1/traces/$TID/revisions/2/replay?policy_version=1"
# => verdict / stored_verdict / matches_published
```

### 4.6 空闲窗口与硬水位线（无需等待真实时间）

- 不调用 `/end` 等待空闲窗口到期，后台扫描器会自动收尾；演示时可立即触发：

```bash
curl -s -X POST localhost:8080/admin/sweep
# => {"watermarked": N, "window_closed": M}
```

- 超过硬水位线仍未闭合的 trace 被标记为 `INDETERMINATE`，证据中包含
  `would_have_verdict`（规则本会给出的结论）与传播链。

## 5. 策略 DSL

```jsonc
{
  "default_decision": "DISCARD",      // 无规则命中时的默认结论
  "propagation": {
    "error_to_parents": true,         // 错误状态向祖先传播
    "error_to_children": false,       // 错误状态向后代传播
    "error_link_marks": "source"      // none | source | target
  },
  "rules": [
    {
      "id": "r1",
      "priority": 100,                // 数字越大越先匹配；首个命中规则决定结论
      "scope": "any",                 // any | all | root
      "decision": "RETAIN",           // RETAIN | DISCARD
      "match": {
        "status": "ERROR",
        "min_duration_ms": 1000,
        "max_duration_ms": 60000,
        "name_equals": "charge",
        "name_matches": "^GET /.*",
        "attributes": [{"key": "k", "op": "eq", "value": "v"}],
        "parent": { "...同一谓词..." },
        "child":  { "...同一谓词..." },
        "linked": { "status": "ERROR", "attributes": [ ... ] }
      }
    }
  ]
}
```

属性操作符 `op`：`eq / ne / gt / gte / lt / lte / in / nin / exists / contains / prefix`。
- `scope=all`：要求 trace 中**所有** span 都匹配（空 trace 不命中）。
- `scope=root`：只评估根 span（找不到父 span 的孤儿 span 也视为根；环中 span 不算根）。
- 传播是对**有效错误状态**做不动点扩散，`error_sources` 会记录每个 span 错误状态的来源链。

## 6. 测试

纯引擎单元测试（无需数据库）：

```bash
pip install -r requirements.txt
pytest -q tests/test_engine.py
```

完整集成测试（需要 PostgreSQL，推荐在 compose 内执行）：

```bash
docker compose up --build -d
docker compose exec api pytest -q
```

覆盖：乱序重建、环/孤儿、错误传播（父/子/link）、延迟与属性组合、父子嵌套谓词、
幂等同参/异参（含**真实 HTTP 并发**：在独立 uvicorn 进程 + 真实 TCP/线程下验证
异参请求在慢业务事务期间立即 409 的**响应时序**、同参业务只执行一次且响应完全一致、
首请求失败占位转 FAILED 后受控重试、崩溃残留 PENDING 占位接管；以及**受控交错**回归：
首写业务超过 PENDING TTL 后“先读处理中行、首写随后提交”的接管竞态——已完成行保留
首写指纹/状态码/响应体，异参 409、同参重放、业务不重复执行、账本 `attempts` 不被误增，
覆盖 spans/end/policies 全部幂等写入口；FAILED 占位多请求**同时重选举**只产生一位
重试领导者且其余请求重放其响应）、span 冲突、
迟到 span → 新 revision 且旧决定不可变、并发收尾只归档一次、
CAS 冲突与发布（含幂等键只发布一次）、窗口关闭、水位线 `INDETERMINATE`、
按 revision 重放。

> 说明：真实 HTTP 并发测试通过 `TS_ENABLE_TEST_HOOKS=true` 启用 `/admin/test/*`
> 故障注入端点来“放慢业务事务”；Docker 镜像与 `docker-compose.yml` 不设置该变量，
> 生产运行时这些端点不存在。

## 7. 数据模型概览

- `traces` — 主行（state、当前 revision、当前 decision 指针；收尾时 `SELECT … FOR UPDATE`）
- `spans` — span 明细，带到达 revision；`(trace_id, span_id)` 唯一
- `policies` — 不可变版本化策略，`is_active` 指向当前版本
- `decisions` — 每个 `(trace_id, revision)` 一行决策 + JSONB 证据，唯一约束防重复归档
- `idempotency_keys` — 幂等键账本：`(scope, key)` 唯一。首请求在**独立的短事务**
  以 `INSERT ... ON CONFLICT DO NOTHING` 原子写入 **PENDING 占位并立即提交**
  （SHA-256 请求指纹 `request_hash`、本次属主标识 `run_token`），因此占位在业务
  运行期间即可被并发请求看到：异参立即 409、同参随后进入业务事务在占位行上
  `SELECT … FOR UPDATE` 等待；业务事务成功时同事务翻转 `COMPLETED` 并保存原
  状态码/响应体，业务失败则经 SAVEPOINT 回滚后同事务标记 `FAILED`，由等待的同参
  请求重选一位接管重试。`attempts` 记录崩溃残留占位被接管的次数，`locked_at`
  配合 TTL 做崩溃恢复判定；接管探测在 `FOR UPDATE` 行锁保护下重读权威行
  （`populate_existing`），已完成行不会被误接管改写
