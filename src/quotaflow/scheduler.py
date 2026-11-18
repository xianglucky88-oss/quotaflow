"""公平调度器：在账本额度约束内决定"先抓谁、花多少"。

职责边界
========
- 额度约束（绝不超额、车道公平份额）只在 budget.BudgetLedger 内表达，
  调度器不自己计数，只按账本返回的 BudgetExceeded/LaneExceeded 退让；
- 本模块负责排序与重试编排，排序依据三类信息：
    1. 接口优先级（policy.LaneSpec.weight，越大越优先）；
    2. 剩余时间（距截止时刻越近越紧迫，deadline 缺省视为最不紧迫）；
    3. 请求成本（同等条件下低成本先跑，紧张额度下可完成率更高）；
  同优先级在机构间按机构名轮转，避免单一机构饿死其它机构。
- 每个逻辑请求的 request_id 由 keyer.request_key 确定性派生：
  成功响应丢失后的超时重试、进程重启后的续跑都复用同一 request_id，
  账本侧幂等返回同一笔预留，重复扣费在数据模型层即不可能发生。
- 重试次数不依赖内存：从该 request_id 的 HOLD/REAP 事件数派生，
  进程重启后仍可复算；达到作业声明的上限后停止再排。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from . import incremental, keyer, policy as policy_mod
from .budget import BudgetExceeded, BudgetLedger, LaneExceeded, utc_now

MAX_ATTEMPTS = 5
_RETRY_BACKOFF_SECONDS = (0, 1, 3, 8, 20)


@dataclass(frozen=True)
class Job:
    org: str
    endpoint: str
    lane: str
    cost: int
    params: dict[str, Any] = field(default_factory=dict)
    deadline: datetime | None = None  # tz-aware；None 表示无截止时间
    max_attempts: int = MAX_ATTEMPTS
    # 轮次标签：None 表示由调度器按已持久化游标自动派生
    round_tag: str | None = None

    def request_id_with(self, round_tag: str) -> str:
        return keyer.request_key(self.org, self.endpoint, self.params,
                                 round_tag=round_tag)

    @property
    def request_id(self) -> str:
        """无游标场景下的稳定键（测试与单轮作业使用）。"""
        return self.request_id_with(self.round_tag or "")


@dataclass(frozen=True)
class PlannedRequest:
    """已通过账本预留、可以发出的请求计划。"""

    job: Job
    request_id: str
    attempt: int
    lease_owner: str
    headers: dict[str, str]


@dataclass(frozen=True)
class Deferred:
    job: Job
    reason: str
    detail: str


@dataclass
class ScheduleResult:
    planned: list[PlannedRequest] = field(default_factory=list)
    deferred: list[Deferred] = field(default_factory=list)
    held_for_reconcile: list[str] = field(default_factory=list)

    @property
    def request_ids(self) -> list[str]:
        return [p.request_id for p in self.planned]


class Scheduler:
    def __init__(
        self,
        ledger: BudgetLedger,
        *,
        owner: str | None = None,
        now: Callable[[], datetime] = utc_now,
        retry_backoff: tuple[int, ...] = _RETRY_BACKOFF_SECONDS,
    ) -> None:
        self.ledger = ledger
        self.owner = owner or f"sched-{uuid.uuid4().hex[:8]}"
        self._now = now
        self._backoff = retry_backoff
        incremental.init(ledger.con)

    # ------------------------------------------------------------------ #
    def _weight(self, job: Job) -> int:
        return self.ledger.policies.get(job.org).lane(job.lane).weight

    def _ordered(self, jobs: list[Job], now: datetime) -> list[Job]:
        """优先级降序 → 紧迫度升序 → 机构轮转（机构名）→ 成本升序。"""
        return sorted(
            jobs,
            key=lambda j: (
                -self._weight(j),
                0 if j.deadline is not None else 1,
                (j.deadline - now).total_seconds() if j.deadline is not None else 0.0,
                j.org,
                j.cost,
            ),
        )

    # ------------------------------------------------------------------ #
    def recover(self) -> list[str]:
        """进程重启入口：回收过期租约，汇总需要对账的在途请求。

        回收后孤儿预留变为 HELD（额度仍挂账），结合游标中记录的
        last_request_id，调用方可对同一 request_id 做条件重试或凭据退款。
        """
        reaped = self.ledger.reap_orphans(now=self._now())
        held = [r.request_id for r in self.ledger.reservations_in_state(policy_mod.STATE_HELD)]
        seen: set[str] = set()
        ordered: list[str] = []
        for rid in reaped + held:
            if rid not in seen:
                seen.add(rid)
                ordered.append(rid)
        return ordered

    def attempt_of(self, request_id: str) -> int:
        """下一次尝试序号 = 1 + 既往 HOLD/REAP 次数（持久化派生，可复算）。"""
        rows = self.ledger.con.execute(
            "SELECT event FROM budget_event WHERE request_id=?", (request_id,)
        ).fetchall()
        retries = sum(1 for r in rows if r["event"] in (policy_mod.EVENT_HOLD, policy_mod.EVENT_REAP))
        return retries + 1

    def backoff_seconds(self, attempt: int) -> int:
        idx = min(max(attempt - 1, 0), len(self._backoff) - 1)
        return self._backoff[idx]

    def retry_after(self, attempt: int) -> datetime:
        """计算下一次重试时刻（仅时间编排，不动账本）。"""
        return self._now() + timedelta(seconds=self.backoff_seconds(attempt))

    # ------------------------------------------------------------------ #
    def schedule(self, jobs: list[Job]) -> ScheduleResult:
        """对一批作业做一轮公平调度并完成账本预留。

        额度不足的作业进入 deferred（带原因），不阻塞其它机构/车道的作业；
        同一 request_id 重复进入时走账本幂等分支，不产生第二笔预留。
        """
        result = ScheduleResult()
        now = self._now()
        for job in self._ordered(list(jobs), now):
            # 轮次标签：显式指定优先，否则取已持久化游标；
            # 游标推进后自动换轮，上一轮终结的 request_id 不会被复活。
            cursor = incremental.load_cursor(self.ledger.con, job.org, job.endpoint)
            round_tag = job.round_tag if job.round_tag is not None else (
                cursor.cursor if cursor and cursor.cursor else "")
            request_id = job.request_id_with(round_tag)

            attempt = self.attempt_of(request_id)
            if attempt > job.max_attempts:
                result.deferred.append(
                    Deferred(job, "max_attempts",
                             f"{request_id} 已达 {job.max_attempts} 次上限")
                )
                continue
            try:
                res = self.ledger.reserve(
                    job.org,
                    job.lane,
                    request_id,
                    job.cost,
                    endpoint=job.endpoint,
                    owner=self.owner,
                )
            except (BudgetExceeded, LaneExceeded) as exc:
                kind = "budget_exceeded" if isinstance(exc, BudgetExceeded) else "lane_exceeded"
                result.deferred.append(Deferred(job, kind, str(exc)))
                continue

            headers = cursor.conditional_headers() if cursor else {}
            result.planned.append(
                PlannedRequest(
                    job=job,
                    request_id=res.request_id,
                    attempt=attempt,
                    lease_owner=self.owner,
                    headers=headers,
                )
            )
            incremental.save_cursor(
                self.ledger.con,
                job.org,
                job.endpoint,
                last_request_id=res.request_id,
                last_status="PLANNED",
            )
        result.held_for_reconcile = self.recover()
        self.ledger.assert_healthy()
        return result


# ---- 流水线阶段兼容 ----------------------------------------------------
def process(record):
    rec = dict(record)
    rec.setdefault("_stage_scheduler", "fair")
    return rec
