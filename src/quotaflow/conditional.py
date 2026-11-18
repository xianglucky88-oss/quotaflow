"""条件请求环节：把 HTTP 结果翻译成账本动作。

结果分类与账本映射（依据 RFC 9110 语义与本项目 policy.REFUND_EVIDENCE）
=====================================================================

| 结果                | 语义依据                     | 账本动作                              |
|---------------------|------------------------------|---------------------------------------|
| 304 Not Modified    | 条件命中，正文未变化         | settle(conditional_cost)，游标不推进  |
| 200 OK              | 全量返回                     | settle(预留成本)，推进游标/ETag       |
| 206 Partial Content | 部分响应，按实际片段计费     | settle(partial_units)（不得超预留）   |
| 429 Too Many Requests | 服务端拒绝处理，未消耗     | refund(rate_limited, 凭据含 retry_after) |
| 其它 4xx            | 请求被拒，服务端未处理       | refund(client_rejected, 凭据含 status)|
| 408 / 5xx / 超时 / 断连 | 结果未知，可能已被受理   | hold()，额度继续挂账，等待对账        |

关键纪律：任何"结果未知"都不允许调用 refund()。账本自身也会二次校验
理由白名单与凭据字段，本模块与账本构成双重防线。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from . import incremental, policy as policy_mod
from .budget import BudgetLedger, RefundNotAllowed


class TransportError(Exception):
    """传输层异常（连接错误/超时），语义上等于结果未知。"""


class TimeoutError(TransportError):  # noqa: A001 - 领域内显式命名
    """请求超时：远端可能已受理，结果未知。"""


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    partial_units: int | None = None  # 206 时由调用方/传输层给出实际片段单位

    def header(self, name: str) -> str | None:
        lowered = {k.lower(): v for k, v in self.headers.items()}
        return lowered.get(name.lower())


class Transport(Protocol):
    def send(self, org: str, endpoint: str, params: dict[str, Any], headers: dict[str, str]) -> Response:
        ...


@dataclass(frozen=True)
class Outcome:
    request_id: str
    action: str          # settled / refunded / held
    state: str
    detail: dict[str, Any]


class ConditionalProcessor:
    """执行计划请求并把结果逐笔过账。与 Scheduler 共享同一个 BudgetLedger。"""

    def __init__(self, ledger: BudgetLedger, transport: Transport) -> None:
        self.ledger = ledger
        self.transport = transport
        incremental.init(ledger.con)

    # ------------------------------------------------------------------ #
    def execute(self, plan) -> Outcome:
        """plan: scheduler.PlannedRequest。"""
        job = plan.job
        headers = dict(plan.headers)
        # 条件头在调度期已从游标装入；此处再确保幂等键可被远端用于去重
        headers.setdefault("Idempotency-Key", plan.request_id)
        try:
            resp = self.transport.send(job.org, job.endpoint, job.params, headers)
        except TimeoutError as exc:
            return self._hold(plan, "timeout", {"error": str(exc)})
        except TransportError as exc:
            return self._hold(plan, "connection_error", {"error": str(exc)})
        return self.apply_response(plan, resp)

    # ------------------------------------------------------------------ #
    def apply_response(self, plan, resp: Response) -> Outcome:
        """纯规则入口：给定计划与响应，执行对应账本动作（也便于离线测试）。"""
        job = plan.job
        rid = plan.request_id
        status = resp.status

        if status == 304:
            # RFC 9110 15.4.6：304 表示验证条件不满足（资源未变），无响应正文
            res = self.ledger.settle(
                rid,
                self.ledger.policies.get(job.org).conditional_cost,
                reason=policy_mod.SETTLE_NOT_MODIFIED,
                evidence={"status": 304},
            )
            incremental.save_cursor(
                self.ledger.con, job.org, job.endpoint,
                last_request_id=rid, last_status="NOT_MODIFIED",
            )
            return Outcome(rid, "settled", res.state,
                           {"status": 304, "actual_cost": res.actual_cost})

        if status == 200:
            res = self.ledger.settle(
                rid, job.cost, reason=policy_mod.SETTLE_FULL,
                evidence={"status": 200},
            )
            self._advance_cursor(plan, resp)
            return Outcome(rid, "settled", res.state,
                           {"status": 200, "actual_cost": res.actual_cost})

        if status == 206:
            # 部分响应：以实际片段单位结算，差额由账本按 settle_remainder 释放
            units = resp.partial_units
            if units is None:
                raise ValueError("206 Partial Content 必须提供 partial_units")
            if not (0 <= units <= job.cost):
                raise ValueError(f"partial_units {units} 超出预留 {job.cost}")
            res = self.ledger.settle(
                rid, units, reason=policy_mod.SETTLE_PARTIAL,
                evidence={"status": 206, "partial_units": units},
            )
            self._advance_cursor(plan, resp, allow_partial=True)
            return Outcome(rid, "settled", res.state,
                           {"status": 206, "actual_cost": res.actual_cost})

        if status == 429:
            # RFC 9110 15.5.5.2.6：服务端拒绝提供服务；retry_after 是必需凭据
            retry_after = resp.header("retry-after") or ""
            res = self.ledger.refund(
                rid, "rate_limited",
                {"status": 429, "retry_after": retry_after},
            )
            incremental.save_cursor(
                self.ledger.con, job.org, job.endpoint,
                last_request_id=rid, last_status="RATE_LIMITED",
            )
            return Outcome(rid, "refunded", res.state,
                           {"status": 429, "retry_after": retry_after})

        if 400 <= status < 500:
            # 408 请求超时语义上结果未知，不允许退款
            if status == 408:
                return self._hold(plan, "http_408", {"status": 408})
            res = self.ledger.refund(
                rid, "client_rejected", {"status": status}
            )
            incremental.save_cursor(
                self.ledger.con, job.org, job.endpoint,
                last_request_id=rid, last_status=f"CLIENT_{status}",
            )
            return Outcome(rid, "refunded", res.state, {"status": status})

        if status >= 500:
            return self._hold(plan, "http_5xx", {"status": status})

        # 未覆盖的状态码按未知处理：宁可挂起也不误放额度
        return self._hold(plan, "manual", {"status": status})

    # ------------------------------------------------------------------ #
    def _hold(self, plan, reason: str, evidence: dict[str, Any]) -> Outcome:
        res = self.ledger.hold(plan.request_id, reason, evidence)
        incremental.save_cursor(
            self.ledger.con, plan.job.org, plan.job.endpoint,
            last_request_id=plan.request_id, last_status=f"HELD:{reason}",
        )
        return Outcome(plan.request_id, "held", res.state,
                       {"reason": reason, "evidence": evidence})

    def _advance_cursor(self, plan, resp: Response, *, allow_partial: bool = False) -> None:
        etag = resp.header("etag")
        last_modified = resp.header("last-modified")
        next_cursor = resp.header("x-next-cursor")
        if allow_partial and next_cursor is None:
            # 部分响应且没有新游标时不推进位置，仅记录条件头
            next_cursor = None
        incremental.save_cursor(
            self.ledger.con,
            plan.job.org,
            plan.job.endpoint,
            cursor=next_cursor,
            etag=etag,
            last_modified=last_modified,
            last_request_id=plan.request_id,
            last_status="PARTIAL" if allow_partial else "OK",
        )

    # ------------------------------------------------------------------ #
    def reconcile_refund(self, request_id: str, reason: str, evidence: dict[str, Any]) -> Outcome:
        """人工/对账通道：确认远端未受理后，为 HELD 预留补齐凭据退款。

        凭据校验仍由账本强制执行（RefundNotAllowed 直接上抛），
        本方法不提供任何绕过白名单的捷径。
        """
        res = self.ledger.refund(request_id, reason, evidence)
        return Outcome(request_id, "refunded", res.state,
                       {"reason": reason, "evidence": evidence})

    def assert_refund_blocked(self, request_id: str, reason: str, evidence: dict[str, Any]) -> str:
        """测试/审计辅助：确认某理由确实无法退款，返回错误信息。"""
        try:
            self.ledger.refund(request_id, reason, evidence)
        except RefundNotAllowed as exc:
            return str(exc)
        raise AssertionError(f"{request_id} 本应拒绝退款理由 {reason}，但退款成功")


def process(record):
    """流水线阶段兼容接口（真实处理由 ConditionalProcessor 驱动）。"""
    rec = dict(record)
    rec.setdefault("_stage_conditional", "etag")
    return rec
