# quotaflow — 多机构 API（配额/游标/ETag），每日预算内编排抓取与增量同步。

运行栈：Python 3.11+ 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
两条入口共用同一套规范记录与稳定键：

```bash
make run     # 旧路径：fixtures/input.json 裸记录 → normalize→budget→scheduler → keyer → 幂等落库
make sync    # 新路径：fixtures/responses.json 多机构响应批次 → normalize 整批 → conditional → keyer/store → incremental
make test    # 运行项目测试（42 项：规范化语义、条件比较、游标/中断恢复、幂等）
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`conditional`, `incremental`, `scheduler`, `budget`, `normalize`。
通用模块：`loader / normalize / keyer / store / rawarchive / pipeline / cli`。

## normalize：多机构响应的统一语义
`normalize.normalize_response(payload, source)` 把任意机构的一次响应收敛为
`NormalizedResponse`（`models.py`），包含三类产物，分别供不同下游消费：

1. **规范资源记录 `records`**：统一字段 `id/src/name/value/status/ts/etag/updated_at/deleted/schema_version`，
   可直接被现有 keyer/store 稳定落库；
2. **来源字段映射与原文摘要**：记录内 `_provenance.field_map`（机构字段 → 规范字段）、
   `extra_fields`（未知字段保留）、`raw_digest`；原文存于 `rawarchive` 表，按 sha256 摘要回查；
3. **响应状态与规范游标**：`status`、`PageCursor`、`etag`、`updated_at`，
   供 `conditional` 缓存比较与 `incremental` 游标推进。

### 响应/记录状态：哪些可同步，哪些只能隔离
| 形态 | 统一结果 | 同步语义 |
| --- | --- | --- |
| 正常资源页 | `status=ok`，记录入 `records` | 可同步，走 keyer/store |
| 机构空结果（空列表/空信封） | `status=empty`，无记录 | 不产出资源，但**游标可推进** |
| 错误包（`error`/`error_code`/`success=false`/HTTP 4xx5xx） | `status=error` + 隔离项 `error_packet` | **不可同步**，游标不推进，下轮原游标重试 |
| 不可解析内容 / 未知信封 | 隔离项 `unparseable` / `unknown_envelope` | 不可同步 |
| 新 schema 主版本（如 v9） | 隔离项 `schema_blocked` | 阻断，不猜测语义 |
| 记录缺资源标识 | 合成 `synth:<指纹>` 标识 | 可同步，指纹对重复同步稳定 |
| 记录未知状态值 | 隔离项 `malformed_record`（仅该记录） | 同页其他记录不受影响 |
| 删除标记 tombstone | 记录 `deleted=true`（可带原状态） | **是资源状态，可同步**，不是错误包 |

### 统一规则
- **分页游标**：`PageCursor(next_cursor, has_more, page_index, page_size, raw_cursor)`。
  `next_cursor=None` 且 `has_more=false` 表示末页；机构自相矛盾时以 `has_more` 为准，原 token 留档。
  乱序页在 `normalize_batch` 按 `(source, page_index)` 重排，页序号缺口写入 `order_gaps`。
- **时间戳**：全部转 UTC ISO-8601（`Z` 结尾），覆盖 `Z`/偏移量/naive（按机构默认时区）/epoch 秒毫秒/仅日期；
  不可解析置 `null` 并记 note，不中断整页。
- **ETag**：trim 归一，保留弱验证前缀 `W/`；空串/`null` 统一为 `None`。
- **空值**：字段级 `null` 宽松归一（如 `value` 缺省补 0、不可解析置 null）并在 notes 留痕；
  机构级“无资源”统一为 `empty`。
- **稳定键**：`keyer.key_of` = `src:id`（跨机构命名空间隔离），无 `src` 时保持旧实现不变；
  合成标识同样稳定，错误包/隔离项永远到不了 keyer。

### 兼容边界（未知字段与新 schema 版本）
- **未知字段：保留不阻断**。收入 `_provenance.extra_fields`，规范字段不受影响。
- **schema 版本**：已知主版本 v1/v2 全支持（含 v1.7 等旧 minor）；同主版本的更新 minor 默认沿用解析器，
  未知字段按上一条保留；**未知主版本整页阻断并隔离**（`SUPPORTED_SCHEMA_MAJORS` 显式登记）。
- 旧裸记录入口 `normalize.process(record)` 与旧 `make run` 结果保持不变（fed=4、rows=3）。

## 下游消费契约
- **conditional.py**：按 ETag（强/弱同 body 即不变）→ `updated_at`/`ts` 顺序比较，
  输出 `fetch / unchanged / changed / quarantine` 决策；错误响应直接 `quarantine`。
- **incremental.py**：每机构持久化 `sync_state(last_cursor,last_page_index,finished,last_etag,...)`。
  只有 ok/empty 响应推进游标；error 不推进；页序号必须连续，乱序/缺口页拒绝推进；
  已应用页重放为幂等 no-op。状态与记录落库共用同一 sqlite 事务，中断后重放不产生重复行。
- **keyer/store**：只消费规范记录，upsert 幂等；`store.get` 供 conditional 读取缓存。
- **rawarchive.py**：来源原文内容寻址归档，与业务表分离，错误包也能按摘要回查但不进资源表。

## 持久化与测试
SQLite 表：`records`（资源）、`sync_state`（游标）、`raw_archive`（原文）。
测试夹具位于 `fixtures/` 与 `tests/`，测试统一使用临时库（`tests/conftest.py`），不污染项目数据库。
