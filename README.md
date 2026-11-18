# quotaflow — 多机构 API（配额/游标/ETag），每日预算内编排抓取与增量同步。

运行栈：Python 3.11+ 标准库 `sqlite3` / `zoneinfo` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalize` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试（账本、并发抢占、跨时区日界、SIGKILL 崩溃恢复、重放对账）
```

账本独立入口：
```text
PYTHONPATH=src python3 -m quotaflow.cli verify   # 打开持久化账本做离线对账
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。

| 模块 | 职责 |
|------|------|
| `policy.py` | 机构策略：IANA 时区、日限额、优先级车道权重、退款理由白名单与必需凭据（纯策略无 IO） |
| `budget.py` | 持久化账本：预留/结算/退款/挂起/租约回收，账户恒等式与事件流水，离线对账 `verify()/replay()` |
| `scheduler.py` | 公平调度：按优先级、剩余时间紧迫度、成本排序，跨机构轮转，调用账本预留并重试编排 |
| `conditional.py` | 条件请求：304/200/206/429/4xx/5xx/超时 → 结算/退款/挂起的账本映射（RFC 9110 语义） |
| `incremental.py` | 游标、ETag、Last-Modified 与在途 request_id 的持久化，支撑重启恢复 |
| `keyer.py` | 记录主键与逻辑请求幂等键 `request_key`（同键重试不双扣，游标推进自动换轮） |
| `normalize` / `loader` / `store` / `pipeline` / `cli` | 通用加载、规范化、存储与薄编排 |

## 两条关键规则（依据见 docs/ledger.md）
1. **跨日重置**：按每个机构配置的 IANA 时区之本地午夜划分配额日，懒开账、无重置作业；
   迟到结算按 request_id 写回旧日，不能挪占新日额度。
2. **未知结果不退款**：超时/5xx/408/断连一律 HELD 挂账，只有凭据齐全
   （429 带 retry-after、对账反查未受理等）才允许全额退款。

## 持久化与测试
SQLite（WAL + `BEGIN IMMEDIATE`）保证并发串行化与原子提交；事件只追加，
`verify()` 重放流水核对计数器与资源上限。`tests/test_durability.py` 以真实子进程
SIGKILL 验证半写入回滚、孤儿租约回收与旧输入重放。
