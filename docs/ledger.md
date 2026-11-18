# 预算账本设计依据与状态机

本文件记录 `budget.py` / `scheduler.py` / `conditional.py` / `incremental.py`
共同遵守的规则及其依据。代码中的策略集中在 `policy.py`，账本只执行策略、不自行决策。

## 1. 跨日重置采用哪个机构时区？

**规则**：每个机构在 `PolicyBook` 中配置一个 IANA 时区名（如 `Asia/Shanghai`、
`America/Los_Angeles`），配额日分界线取该时区的本地午夜 00:00；未配置时回退 UTC。

**依据**：

1. 多机构 API 的配额通常按"机构所在地营业日"重置，机构配置自身时区即归属该营业日；
2. IANA 时区名携带完整夏令时规则。固定 UTC 偏移（如 `+08:00`）无法表达 DST，
   洛杉矶 2026-11-01 夏令时结束当天，固定偏移会让重置点漂移一小时；
   `zoneinfo.ZoneInfo` 在 DST 切换日给出正确的本地日期（测试见
   `test_dst_boundary_local_day`）；
3. 重置不是批量 UPDATE，而是**懒开账**：新配额日的首次预留按
   `instant.astimezone(ZoneInfo(tz)).date()` 计算 `day` 并新建账户行，
   因此不存在"重置时刻无人运行导致漏重置"的问题；
4. **迟到结算永远写回预留所属的旧日**：`settle/refund/hold` 全部按
   `request_id` 定位预留行，沿用其 `day` 字段，不按当前时间重算日界，
   无法把昨日消耗挪到今日额度内。

## 2. 未知请求结果何时可以退款？

**规则**：只有取得"远端未消耗或已退还额度"的凭据时才允许 `refund()`；
超时、408、5xx、连接错误等结果未知的情况只能 `hold()` 挂起，额度继续占用。

**依据**：

| 场景 | 语义依据 | 处理 | 必需凭据 |
|------|----------|------|----------|
| 304 Not Modified | RFC 9110 §15.4.6：条件命中，无响应正文 | 按 `conditional_cost` 结算 | status |
| 200 OK | 全量返回 | 按预留成本结算 | status |
| 206 Partial | 部分响应 | 按实际片段单位结算，差额自动释放 | status, partial_units |
| 429 Too Many Requests | RFC 9110 §15.5.5.2.6：服务端拒绝处理 | 全额退款 | status, retry_after |
| 其它 4xx（408 除外） | 请求被拒、服务端未处理 | 全额退款 | status |
| 408 / 5xx / 超时 / 断连 | **结果未知，远端可能已受理** | 挂起 HELD | —（仅记录证据） |
| 人工对账确认未受理 | 幂等键反查 | 全额退款 | lookup_request_id, lookup_status |

核心推理：结果未知时若释放额度，重试请求将脱离额度约束——远端实际已受理的情况下
会造成真实超额。因此账本在 `refund()` 内强制校验理由白名单
（`policy.REFUND_EVIDENCE`）与凭据字段非空，`conditional.py` 与账本构成双重防线，
任何代码路径都无法以 `timeout` 之类理由退款（测试见
`test_unknown_result_holds_quota_and_blocks_refund`）。

## 3. 预留状态机

```text
                 reserve(request_id, cost)
                           │
                           ▼
                      ┌─────────┐  settle(actual_cost)   ┌─────────┐
                      │RESERVED │───────────────────────▶│ SETTLED │ 终态
                      │ 租约在途 │  refund(凭据齐全)      └─────────┘
                      └──┬───┬──┘───────────────────────▶┌─────────┐
                         │   │                            │REFUNDED │ 终态
            hold/租约回收 │   └──────────────────────────▶└─────────┘
                         ▼
                      ┌──────┐  settle / refund(对账后)  → 终态
                      │ HELD │  额度继续挂账，绝不自动释放
                      └──────┘
```

- `request_id` 由 `keyer.request_key(org, endpoint, params, round_tag)` 确定性派生，
  同一逻辑请求的超时重试、进程重启续跑复用同一键，`reserve()` 幂等返回同一笔预留；
- `round_tag` 区分"同页重试"与"游标推进后的新一轮"：前者同键不双扣，后者新键；
- 已终结的 `request_id` 不可复活，新逻辑请求必须生成新键；
- 预留带租约（`lease_owner` + `lease_expires_at`）。进程退出后
  `reap_orphans()` 把过期预留回收为 HELD——**只改状态不动计数器**，额度不丢；
- 重试次数不存内存：`Scheduler.attempt_of()` 从该请求的 HOLD/REAP 事件数派生，
  重启后可复算。

## 4. 账户恒等式与公平分配

每个（机构, 配额日）账户与每条（机构, 配额日, 车道）车道行同时满足：

```text
reserved_total = released_total + spent_total + inflight_total
净占用 reserved_total - released_total <= limit（车道为 cap）
```

- `inflight_total` = 当日所有 RESERVED/HELD 行 `held_units` 之和；
- 车道份额 `cap_i = floor(limit * w_i / Σw)`，取整余数按权重降序补发，
  总和严格等于日限额；份额不跨车道挪用，以可预测性换取绝不超额；
- 所有写操作在单条 `BEGIN IMMEDIATE` 事务内同时改计数器、预留行与只追加事件流水，
  sqlite 原子提交保证不存在半写入；WAL + `busy_timeout` 让并发写串行化，
  最后一个单位有且仅有一个赢家（测试见 `test_concurrent_race_for_last_units`
  与跨进程测试）。

## 5. 离线对账（`ledger.verify()` / `python -m quotaflow.cli verify`）

检查八项：计数器非负与恒等式、机构/车道净占用上限、车道汇总等于账户、
预留行金额合法性、在途汇总等于 `inflight_total`、**事件流水重放聚合等于计数器**、
预留配额日与机构时区复核、预留状态机与事件序列相容。
崩溃测试以真实子进程 `SIGKILL` 验证事务中途杀死后重开账本无半行、无丢预留。
