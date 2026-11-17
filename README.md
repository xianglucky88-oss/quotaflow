# quotaflow — 多机构 API（配额/游标/ETag），每日预算内编排抓取与增量同步。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalize` → 其余已实现变换 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线 happy-path，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线、幂等和边界桩
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`conditional`, `incremental`, `scheduler`, `budget`, `normalize`。
通用模块：`loader / normalize / keyer / store / pipeline / cli`。

## 当前实现状态
- `conditional.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `incremental.py`：`process()` 当前抛出 `NotImplementedError`，未接入基础 happy-path。
- `scheduler.py`：提供基础 `process()`，复制输入记录并补充 `_stage_scheduler` 阶段标记。
- `budget.py`：提供基础 `process()`，复制输入记录并补充 `_stage_budget` 阶段标记。
- `normalize.py`：提供可运行的 `process()` 实现，由现有流水线按模块顺序调用。

当前未实现阶段未进入基础流水线，因此 `make run` 仍可运行已接入的阶段。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
