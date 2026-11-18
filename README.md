# quotaflow — 多机构 API（配额/游标/ETag），每日预算内编排抓取与增量同步。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalize` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线和幂等性检查
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`conditional`, `incremental`, `scheduler`, `budget`, `normalize`。
通用模块：`loader / normalize / keyer / store / pipeline / cli`。

## 模块说明
业务模块按独立阶段组织，通用模块负责加载、规范化、主键、存储和编排。
流水线按配置顺序调用处理阶段，输入输出均遵循项目定义的记录结构。

### 调度相关模块

| 模块 | 职责 |
| --- | --- |
| `budget.py` | 每日预算账本：全局上限、机构配额、扣账/退款与流水，其余模块只能通过它扣账。 |
| `incremental.py` | 机构游标：已确认水位 `committed`、未确认区间、部分成功切片、序列化。 |
| `conditional.py` | 条件请求缓存：ETag / Last-Modified、内容指纹、命中预期（`full` / `revalidate` / `miss`）。 |
| `scheduler.py` | 组合以上三者，选出“下一批”任务，记录可恢复、可解释的调度决定。 |

`scheduler.Scheduler` 的 `next_batch()` 同时读取 `budget` 的可用额度、
`incremental` 的未确认区间和 `conditional` 的命中预期。四个阶段模块仍各自导出
`process(record)`，旧 happy-path 流水线行为不变。

### 调度冲突规则（确定性）

候选先过硬门槛（跳过即写入可追查的 `reason`）：`disabled` → `deadline_passed`
→ `circuit_open` → `backoff` → `complete` → `global_budget_exhausted` /
`source_quota_exhausted` → `reserved_budget`。

通过门槛后按固定排序键升序：

```text
(截止紧迫度, 防饥饿位, 重试位, -机构优先级, 预期成本, -已等待轮次, 机构名)
```

- **截止 > 防饥饿 > 重试 > 优先级 > 缓存成本 > 等待轮次 > 名称**；
- 截止紧迫度按“剩余时间 vs 剩余工作量 × 单批耗时”估算，0 = 会错过，1 = 预警，2 = 正常；
- 连续 `starvation_rounds` 轮未被派发的机构进入防饥饿位，可越过更高优先级；
- 上一批失败/部分成功的未确认区间优先重试，不会因为“发过请求”就当成完成；
- `reserve` 为机构保留当天最低请求数；若保底互相卡死，会放行排名最靠前者并记录
  `reservation overridden`，避免死锁。

### 失败、重启与可解释性

- 失败：区间保持未确认，按指数退避设置 `retry_at`；连续失败达阈值后熔断，
  冷却期内跳过该机构，冷却结束后半开探测一次；
- 部分成功：`[start, partial_until)` 确认，其余保持未确认；
- `Scheduler.save(path)` / `Scheduler.load(path)` 原子写入检查点，账本、游标、
  失败计数、等待轮次、批次序号与历史决定都会恢复，重启后继续派发的顺序与未中断
  运行一致；
- `explain()` 给出当前排序与每个机构派发/跳过的原因，`deferrals()` 可按轮次追查
  跳过记录（例如为什么某机构因预算/保底/熔断被跳过）。

`fixtures/schedule_multi.json` 是多机构夹具：交替注入长期失败、部分完成与额度用尽，
`tests/test_scheduler.py` 用它比较重启前后的任务顺序、完成覆盖与请求量
（`tests/conftest.py` 提供 `build` / `run_rounds` / `summary`）。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
