"""预算账本：预留 / 结算 / 退款的唯一事实来源（持久化凭据）。

设计要点（与 scheduler.py、conditional.py 的契约）
====================================================

1. 额度生命周期（每个逻辑请求一条 reservation 行，request_id 即幂等键）::

       reserve()  RESERVED ──settle()──▶ SETTLED   （终态，按实际成本）
                   │   │
                   │   └──refund(凭据)──▶ REFUNDED（终态，全额释放）
                   │
                   └──hold()/租约回收──▶ HELD ──settle/refund──▶ 终态
                                        （结果未知，额度继续挂账，绝不自动释放）

2. 未知请求结果不能直接释放额度：超时、5xx、408、连接错误只允许 hold()；
   refund() 在账本层强制校验理由与凭据（见 policy.REFUND_EVIDENCE），
   理由不在白名单或凭据字段缺失一律 RefundNotAllowed。

3. 账户恒等式（每日、每机构、每车道同时成立）::

       reserved_total = released_total + spent_total + inflight_total
       reserved_total <= limit（车道为 cap）
   inflight_total = 该日所有 RESERVED/HELD 行 held_units 之和。
   跨日重置不产生 UPDATE：新配额日首次预留时按机构时区计算 day 并新开一行，
   迟到结算永远写回预留所属的旧日行（settle 按 request_id 定位，不重新算日）。

4. 崩溃安全：所有写操作在单条 BEGIN IMMEDIATE 事务内同时改计数器、预留行
   与只追加事件流水，sqlite 原子提交保证不会出现"半写入"；
   进程退出后在途预留由 reap_orphans() 按租约回收为 HELD，额度不丢。

5. verify() / replay() 提供离线对账：事件流水分组聚合必须逐分逐厘等于
   计数器，预留行状态必须与其事件序列一致。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from . import policy as policy_mod

SCHEMA_VERSION = 1
STATE_RESERVED = policy_mod.STATE_RESERVED
STATE_HELD = policy_mod.STATE_HELD
STATE_SETTLED = policy_mod.STATE_SETTLED
STATE_REFUNDED = policy_mod.STATE_REFUNDED
TERMINAL_STATES = policy_mod.TERMINAL_STATES

# 允许的状态转移；账本之外不存在任何旁路。
_TRANSITIONS: dict[str, frozenset[str]] = {
    STATE_RESERVED: frozenset({STATE_SETTLED, STATE_REFUNDED, STATE_HELD}),
    STATE_HELD: frozenset({STATE_SETTLED, STATE_REFUNDED}),
    STATE_SETTLED: frozenset(),
    STATE_REFUNDED: frozenset(),
}


class LedgerError(RuntimeError):
    """账本错误基类。"""


class BudgetExceeded(LedgerError):
    """机构当日剩余额度不足。"""


class LaneExceeded(LedgerError):
    """车道保证份额不足（公平分配的硬约束）。"""


class InvalidTransition(LedgerError):
    """预留状态不允许该转移，或重复使用已终结的 request_id。"""


class RefundNotAllowed(LedgerError):
    """退款理由不在白名单，或必需凭据缺失/为空。"""


class LedgerIntegrityError(LedgerError):
    """离线对账发现恒等式被破坏。"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Reservation:
    request_id: str
    org: str
    lane: str
    day: str
    cost: int
    state: str
    held_units: int
    actual_cost: int | None
    reason: str | None
    evidence: dict[str, Any]
    created_at: str
    updated_at: str
    lease_owner: str | None
    lease_expires_at: str | None
    attempt: int


_DDL = """
CREATE TABLE IF NOT EXISTS budget_schema_version(
    version INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS budget_account(
    org             TEXT NOT NULL,
    day             TEXT NOT NULL,
    limit_units     INTEGER NOT NULL,
    reserved_total  INTEGER NOT NULL DEFAULT 0,
    released_total  INTEGER NOT NULL DEFAULT 0,
    spent_total     INTEGER NOT NULL DEFAULT 0,
    inflight_total  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (org, day)
);
CREATE TABLE IF NOT EXISTS budget_lane(
    org             TEXT NOT NULL,
    day             TEXT NOT NULL,
    lane            TEXT NOT NULL,
    cap_units       INTEGER NOT NULL,
    reserved_total  INTEGER NOT NULL DEFAULT 0,
    released_total  INTEGER NOT NULL DEFAULT 0,
    spent_total     INTEGER NOT NULL DEFAULT 0,
    inflight_total  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (org, day, lane)
);
CREATE TABLE IF NOT EXISTS reservation(
    request_id        TEXT PRIMARY KEY,
    org               TEXT NOT NULL,
    lane              TEXT NOT NULL,
    day               TEXT NOT NULL,
    cost              INTEGER NOT NULL CHECK (cost > 0),
    state             TEXT NOT NULL CHECK (state IN ('RESERVED','HELD','SETTLED','REFUNDED')),
    held_units        INTEGER NOT NULL,
    actual_cost       INTEGER,
    reason            TEXT,
    evidence          TEXT NOT NULL DEFAULT '{}',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    lease_owner       TEXT,
    lease_expires_at  TEXT,
    attempt           INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_reservation_state ON reservation(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_reservation_org_day ON reservation(org, day);
CREATE TABLE IF NOT EXISTS budget_event(
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    request_id  TEXT NOT NULL,
    org         TEXT NOT NULL,
    day         TEXT NOT NULL,
    lane        TEXT NOT NULL,
    event       TEXT NOT NULL CHECK (event IN ('RESERVE','SETTLE','REFUND','HOLD','REAP')),
    units       INTEGER NOT NULL,
    detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_event_org_day ON budget_event(org, day, id);
CREATE INDEX IF NOT EXISTS idx_event_request ON budget_event(request_id, id);
"""


def connect(db_path: str = ":memory:") -> sqlite3.Connection:
    """打开账本连接并初始化表结构。

    WAL + busy_timeout：多连接并发抢占最后额度时，写事务串行化，
    锁等待而不是立即失败；配合 BEGIN IMMEDIATE 避免写锁升级死锁。
    """
    con = sqlite3.connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=30000")
    con.executescript(_DDL)
    con.execute(
        "INSERT OR IGNORE INTO budget_schema_version(version) VALUES (?)",
        (SCHEMA_VERSION,),
    )
    con.commit()
    return con


class BudgetLedger:
    """线程内使用一个实例（每个线程/进程持有自己的 sqlite 连接）。"""

    def __init__(
        self,
        con: sqlite3.Connection,
        policies: policy_mod.PolicyBook,
        now: Callable[[], datetime] = utc_now,
        lease_ttl_seconds: int = 300,
    ) -> None:
        self.con = con
        self.policies = policies
        self._now = now
        self._ttl = lease_ttl_seconds

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #
    def _iso(self, dt: datetime | None = None) -> str:
        return (dt or self._now()).astimezone(timezone.utc).isoformat()

    def _txn(self) -> sqlite3.Connection:
        """BEGIN IMMEDIATE，遇到锁则退避重试。"""
        con = self.con
        for delay in (0.02, 0.05, 0.1, 0.2, 0.5):
            try:
                con.execute("BEGIN IMMEDIATE")
                return con
            except sqlite3.OperationalError as exc:  # database is locked
                if "locked" not in str(exc):
                    raise
                import time

                time.sleep(delay)
        con.execute("BEGIN IMMEDIATE")  # 最后一次让异常直接抛出
        return con

    def _ensure_day_rows(self, con: sqlite3.Connection, org: str, day: str) -> None:
        spec = self.policies.get(org)
        con.execute(
            "INSERT OR IGNORE INTO budget_account(org,day,limit_units) VALUES (?,?,?)",
            (org, day, spec.daily_limit),
        )
        caps = spec.lane_caps()
        for lane, cap in caps.items():
            con.execute(
                "INSERT OR IGNORE INTO budget_lane(org,day,lane,cap_units)"
                " VALUES (?,?,?,?)",
                (org, day, lane, cap),
            )

    def _row_to_reservation(self, row: sqlite3.Row) -> Reservation:
        return Reservation(
            request_id=row["request_id"],
            org=row["org"],
            lane=row["lane"],
            day=row["day"],
            cost=row["cost"],
            state=row["state"],
            held_units=row["held_units"],
            actual_cost=row["actual_cost"],
            reason=row["reason"],
            evidence=json.loads(row["evidence"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            attempt=row["attempt"],
        )

    def _get(self, con: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
        return con.execute(
            "SELECT * FROM reservation WHERE request_id=?", (request_id,)
        ).fetchone()

    def _event(
        self,
        con: sqlite3.Connection,
        kind: str,
        res: sqlite3.Row,
        units: int,
        detail: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> None:
        con.execute(
            "INSERT INTO budget_event(ts,request_id,org,day,lane,event,units,detail)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                ts or self._iso(),
                res["request_id"],
                res["org"],
                res["day"],
                res["lane"],
                kind,
                units,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
            ),
        )

    def _require_transition(self, row: sqlite3.Row, target: str) -> None:
        if target not in _TRANSITIONS[row["state"]]:
            raise InvalidTransition(
                f"{row['request_id']}: {row['state']} -> {target} 不允许"
            )

    # ------------------------------------------------------------------ #
    # 1) 预留
    # ------------------------------------------------------------------ #
    def reserve(
        self,
        org: str,
        lane: str,
        request_id: str,
        cost: int,
        *,
        endpoint: str = "",
        owner: str | None = None,
    ) -> Reservation:
        """预留 cost 个配额单位。request_id 重复时按幂等处理。

        幂等规则：
          - 同一 request_id 已存在且参数一致：直接返回既有行（崩溃后重发安全）；
          - 已终结（SETTLED/REFUNDED）的 request_id 不允许再次预留，
            新的逻辑请求必须由调用方生成新 request_id。
        额度判定：机构日限额与车道保证份额同时满足才放行，
        并发下由 BEGIN IMMEDIATE 串行化，最后一个单位只有一个赢家。
        """
        if cost <= 0:
            raise ValueError("reserve cost must be positive")
        spec = self.policies.get(org)
        spec.lane(lane)  # 未知车道提前失败
        now = self._now()
        now_iso = self._iso(now)
        day = self.policies.local_day(org, now)
        owner = owner or f"proc-{uuid.uuid4().hex[:8]}"
        from datetime import timedelta

        lease_exp = self._iso(now + timedelta(seconds=self._ttl))

        con = self._txn()
        try:
            existing = self._get(con, request_id)
            if existing is not None:
                if existing["state"] in TERMINAL_STATES:
                    raise InvalidTransition(
                        f"request_id {request_id!r} 已终结于 {existing['state']}，"
                        "新请求必须使用新 request_id"
                    )
                if (existing["org"], existing["lane"], existing["cost"]) != (
                    org,
                    lane,
                    cost,
                ):
                    raise InvalidTransition(
                        f"request_id {request_id!r} 参数与既有预留不一致"
                    )
                con.commit()
                return self._row_to_reservation(existing)

            self._ensure_day_rows(con, org, day)
            acct = con.execute(
                "SELECT * FROM budget_account WHERE org=? AND day=?", (org, day)
            ).fetchone()
            lane_row = con.execute(
                "SELECT * FROM budget_lane WHERE org=? AND day=? AND lane=?",
                (org, day, lane),
            ).fetchone()
            if acct["reserved_total"] - acct["released_total"] + cost > acct["limit_units"]:
                con.rollback()
                raise BudgetExceeded(
                    f"机构 {org} 配额日 {day} 额度不足："
                    f"需 {cost}，可用 {acct['limit_units'] - acct['reserved_total'] + acct['released_total']}"
                )
            if lane_row["reserved_total"] - lane_row["released_total"] + cost > lane_row["cap_units"]:
                con.rollback()
                raise LaneExceeded(
                    f"机构 {org} 车道 {lane} 份额不足：需 {cost}，"
                    f"可用 {lane_row['cap_units'] - lane_row['reserved_total'] + lane_row['released_total']}"
                )

            con.execute(
                "UPDATE budget_account SET reserved_total=reserved_total+?,"
                " inflight_total=inflight_total+? WHERE org=? AND day=?",
                (cost, cost, org, day),
            )
            con.execute(
                "UPDATE budget_lane SET reserved_total=reserved_total+?,"
                " inflight_total=inflight_total+? WHERE org=? AND day=? AND lane=?",
                (cost, cost, org, day, lane),
            )
            cur = con.execute(
                "INSERT INTO reservation(request_id,org,lane,day,cost,state,"
                "held_units,created_at,updated_at,lease_owner,lease_expires_at,attempt)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    request_id, org, lane, day, cost, STATE_RESERVED, cost,
                    now_iso, now_iso, owner, lease_exp,
                ),
            )
            row = self._get(con, request_id)
            self._event(
                con, policy_mod.EVENT_RESERVE, row, cost,
                {"endpoint": endpoint, "owner": owner, "lease_expires_at": lease_exp},
                ts=now_iso,
            )
            con.commit()
            return self._row_to_reservation(row)
        except Exception:
            con.rollback()
            raise

    # ------------------------------------------------------------------ #
    # 2) 结算（实际消耗，允许部分响应）
    # ------------------------------------------------------------------ #
    def settle(
        self,
        request_id: str,
        actual_cost: int,
        reason: str = policy_mod.SETTLE_FULL,
        evidence: dict[str, Any] | None = None,
    ) -> Reservation:
        """按实际成本结算。actual_cost 必须在 [0, 预留成本] 内。

        多退少不补：实际低于预留的差额作为 REFUND 语义的释放写流水；
        实际高于预留直接拒绝——调用方必须先补预留，账本不允许隐性超额。
        迟到结算安全：按 request_id 定位旧配额日行，不按当前时间重算日界。
        """
        if actual_cost < 0:
            raise ValueError("actual_cost must be >= 0")
        con = self._txn()
        try:
            row = self._get(con, request_id)
            if row is None:
                raise LedgerError(f"未知 request_id: {request_id!r}")
            if actual_cost > row["cost"]:
                raise InvalidTransition(
                    f"{request_id}: 实际成本 {actual_cost} 超过预留 {row['cost']}，"
                    "请先追加预留后再结算"
                )
            if row["state"] == STATE_SETTLED:
                if row["actual_cost"] == actual_cost:
                    con.commit()
                    return self._row_to_reservation(row)  # 幂等重放
                raise InvalidTransition(f"{request_id}: 已结算且成本不一致")
            self._require_transition(row, STATE_SETTLED)
            remainder = row["cost"] - actual_cost
            now_iso = self._iso()
            con.execute(
                "UPDATE budget_account SET released_total=released_total+?,"
                " spent_total=spent_total+?, inflight_total=inflight_total-?"
                " WHERE org=? AND day=?",
                (remainder, actual_cost, row["cost"], row["org"], row["day"]),
            )
            con.execute(
                "UPDATE budget_lane SET released_total=released_total+?,"
                " spent_total=spent_total+?, inflight_total=inflight_total-?"
                " WHERE org=? AND day=? AND lane=?",
                (remainder, actual_cost, row["cost"], row["org"], row["day"], row["lane"]),
            )
            con.execute(
                "UPDATE reservation SET state=?, held_units=0, actual_cost=?,"
                " reason=?, evidence=?, updated_at=? WHERE request_id=?",
                (
                    STATE_SETTLED, actual_cost, reason,
                    json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
                    now_iso, request_id,
                ),
            )
            row = self._get(con, request_id)
            self._event(con, policy_mod.EVENT_SETTLE, row, actual_cost,
                        {"reason": reason, "evidence": evidence or {}}, ts=now_iso)
            if remainder:
                self._event(con, policy_mod.EVENT_REFUND, row, remainder,
                            {"reason": "settle_remainder",
                             "evidence": {"settled_cost": actual_cost}}, ts=now_iso)
            con.commit()
            return self._row_to_reservation(row)
        except Exception:
            con.rollback()
            raise

    # ------------------------------------------------------------------ #
    # 3) 退款（凭据强制；未知结果走不到这里）
    # ------------------------------------------------------------------ #
    def refund(
        self, request_id: str, reason: str, evidence: dict[str, Any]
    ) -> Reservation:
        """全额退款。理由白名单与必需凭据见 policy.REFUND_EVIDENCE。"""
        required = policy_mod.REFUND_EVIDENCE.get(reason)
        if required is None:
            raise RefundNotAllowed(
                f"退款理由 {reason!r} 不在白名单 {sorted(policy_mod.REFUND_EVIDENCE)}；"
                "结果未知的请求只能 hold() 挂起"
            )
        missing = [k for k in required if not evidence or not evidence.get(k)]
        if missing:
            raise RefundNotAllowed(
                f"退款理由 {reason!r} 缺少必需凭据字段: {sorted(required)}（缺 {missing}）"
            )
        con = self._txn()
        try:
            row = self._get(con, request_id)
            if row is None:
                raise LedgerError(f"未知 request_id: {request_id!r}")
            if row["state"] == STATE_REFUNDED:
                if row["reason"] == reason:
                    con.commit()
                    return self._row_to_reservation(row)  # 幂等重放
                raise InvalidTransition(f"{request_id}: 已退款且理由不一致")
            self._require_transition(row, STATE_REFUNDED)
            now_iso = self._iso()
            con.execute(
                "UPDATE budget_account SET released_total=released_total+?,"
                " inflight_total=inflight_total-? WHERE org=? AND day=?",
                (row["cost"], row["cost"], row["org"], row["day"]),
            )
            con.execute(
                "UPDATE budget_lane SET released_total=released_total+?,"
                " inflight_total=inflight_total-? WHERE org=? AND day=? AND lane=?",
                (row["cost"], row["cost"], row["org"], row["day"], row["lane"]),
            )
            con.execute(
                "UPDATE reservation SET state=?, held_units=0, reason=?,"
                " evidence=?, lease_owner=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE request_id=?",
                (
                    STATE_REFUNDED, reason,
                    json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                    now_iso, request_id,
                ),
            )
            row = self._get(con, request_id)
            self._event(con, policy_mod.EVENT_REFUND, row, row["cost"],
                        {"reason": reason, "evidence": evidence}, ts=now_iso)
            con.commit()
            return self._row_to_reservation(row)
        except Exception:
            con.rollback()
            raise

    # ------------------------------------------------------------------ #
    # 4) 未知结果挂起（不释放任何额度）
    # ------------------------------------------------------------------ #
    def hold(
        self, request_id: str, reason: str, evidence: dict[str, Any] | None = None
    ) -> Reservation:
        if reason not in policy_mod.HOLD_REASONS:
            raise LedgerError(
                f"挂起理由 {reason!r} 不合法，允许值: {sorted(policy_mod.HOLD_REASONS)}"
            )
        con = self._txn()
        try:
            row = self._get(con, request_id)
            if row is None:
                raise LedgerError(f"未知 request_id: {request_id!r}")
            now_iso = self._iso()
            if row["state"] == STATE_HELD:
                # 状态幂等：重复挂起不翻转状态，但每次都追加 HOLD 事件，
                # 使重试次数可持续从只追加流水复算（调度器 attempt_of 依赖此性质）。
                self._event(con, policy_mod.EVENT_HOLD, row, row["cost"],
                            {"reason": reason, "evidence": evidence or {},
                             "repeated": True}, ts=now_iso)
                con.commit()
                return self._row_to_reservation(row)
            self._require_transition(row, STATE_HELD)
            now_iso = self._iso()
            con.execute(
                "UPDATE reservation SET state=?, reason=?, evidence=?,"
                " lease_owner=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE request_id=?",
                (
                    STATE_HELD, reason,
                    json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
                    now_iso, request_id,
                ),
            )
            row = self._get(con, request_id)
            self._event(con, policy_mod.EVENT_HOLD, row, row["cost"],
                        {"reason": reason, "evidence": evidence or {}}, ts=now_iso)
            con.commit()
            return self._row_to_reservation(row)
        except Exception:
            con.rollback()
            raise

    # ------------------------------------------------------------------ #
    # 5) 租约回收：进程退出后在途预留 -> HELD（额度不丢、不重复扣）
    # ------------------------------------------------------------------ #
    def reap_orphans(self, *, now: datetime | None = None) -> list[str]:
        """把租约过期且仍为 RESERVED 的预留回收为 HELD。

        回收只改状态不动计数器：inflight 继续挂账，后续对账要么凭据退款、
        要么按确认成本结算，杜绝"进程死了额度凭空释放"。
        """
        now_iso = self._iso(now)
        con = self._txn()
        try:
            rows = con.execute(
                "SELECT * FROM reservation WHERE state=? AND lease_expires_at IS NOT NULL"
                " AND lease_expires_at < ?",
                (STATE_RESERVED, now_iso),
            ).fetchall()
            reaped: list[str] = []
            for row in rows:
                con.execute(
                    "UPDATE reservation SET state=?, reason='lease_expired',"
                    " lease_owner=NULL, lease_expires_at=NULL, updated_at=?"
                    " WHERE request_id=?",
                    (STATE_HELD, now_iso, row["request_id"]),
                )
                fresh = self._get(con, row["request_id"])
                self._event(con, policy_mod.EVENT_REAP, fresh, row["cost"],
                            {"previous_owner": row["lease_owner"]}, ts=now_iso)
                reaped.append(row["request_id"])
            con.commit()
            return reaped
        except Exception:
            con.rollback()
            raise

    # ------------------------------------------------------------------ #
    # 6) 查询
    # ------------------------------------------------------------------ #
    def get(self, request_id: str) -> Reservation | None:
        row = self.con.execute(
            "SELECT * FROM reservation WHERE request_id=?", (request_id,)
        ).fetchone()
        return self._row_to_reservation(row) if row else None

    def reservations_in_state(self, state: str) -> list[Reservation]:
        rows = self.con.execute(
            "SELECT * FROM reservation WHERE state=? ORDER BY created_at", (state,)
        ).fetchall()
        return [self._row_to_reservation(r) for r in rows]

    def day_summary(self, org: str, day: str) -> dict[str, int]:
        row = self.con.execute(
            "SELECT * FROM budget_account WHERE org=? AND day=?", (org, day)
        ).fetchone()
        if row is None:
            return {
                "limit": self.policies.get(org).daily_limit,
                "reserved": 0, "released": 0, "spent": 0, "inflight": 0,
                "available": self.policies.get(org).daily_limit,
            }
        return {
            "limit": row["limit_units"],
            "reserved": row["reserved_total"],
            "released": row["released_total"],
            "spent": row["spent_total"],
            "inflight": row["inflight_total"],
            "available": row["limit_units"] - row["reserved_total"] + row["released_total"],
        }

    def lane_summary(self, org: str, day: str) -> dict[str, dict[str, int]]:
        rows = self.con.execute(
            "SELECT * FROM budget_lane WHERE org=? AND day=? ORDER BY lane", (org, day)
        ).fetchall()
        return {
            r["lane"]: {
                "cap": r["cap_units"],
                "reserved": r["reserved_total"],
                "released": r["released_total"],
                "spent": r["spent_total"],
                "inflight": r["inflight_total"],
                "available": r["cap_units"] - r["reserved_total"] + r["released_total"],
            }
            for r in rows
        }

    def replay(self, org: str | None = None, day: str | None = None) -> list[dict[str, Any]]:
        """读出只追加事件流水（离线重放输入）。"""
        sql = "SELECT * FROM budget_event WHERE 1=1"
        args: list[Any] = []
        if org is not None:
            sql += " AND org=?"
            args.append(org)
        if day is not None:
            sql += " AND day=?"
            args.append(day)
        sql += " ORDER BY id"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    # ------------------------------------------------------------------ #
    # 7) 离线对账：核对旧输入、重放账目与资源上限
    # ------------------------------------------------------------------ #
    def verify(self) -> list[str]:
        """返回违例列表；空列表代表账本健康。

        检查项：
          a. 账户/车道计数器非负且恒等式 reserved = released + spent + inflight；
          b. 机构累计预留不超日限额；车道累计预留不超保证份额；
          c. 车道计数器按日求和恰好等于账户计数器（额度按车道完全划分）；
          d. 预留行 held_units 与状态一致，结算成本不超预留；
          e. 按 request_id 汇总在途金额恰好等于 inflight_total；
          f. 重放事件流水聚合必须等于计数器（无半写入、无丢事件）；
          g. 每个预留的配额日与其创建时刻在机构时区下的本地日一致；
          h. 预留行状态机与事件序列相容。
        """
        problems: list[str] = []
        con = self.con

        accounts = con.execute("SELECT * FROM budget_account").fetchall()
        lanes = con.execute("SELECT * FROM budget_lane").fetchall()

        # a + b：账户
        for a in accounts:
            for col in ("reserved_total", "released_total", "spent_total", "inflight_total"):
                if a[col] < 0:
                    problems.append(f"account {a['org']}/{a['day']} {col} 为负")
            if a["reserved_total"] != a["released_total"] + a["spent_total"] + a["inflight_total"]:
                problems.append(
                    f"account {a['org']}/{a['day']} 恒等式破坏: "
                    f"{a['reserved_total']} != {a['released_total']}+"
                    f"{a['spent_total']}+{a['inflight_total']}"
                )
            # 硬上限看净占用（累计预留 - 已释放）；reserved_total 是累计流水口径，
            # 随退款单调不减，天然可能超过 limit，不构成超额。
            net = a["reserved_total"] - a["released_total"]
            if net > a["limit_units"]:
                problems.append(
                    f"account {a['org']}/{a['day']} 超额: "
                    f"净占用 {net} > limit {a['limit_units']}"
                )

        # a + b：车道
        for ln in lanes:
            for col in ("reserved_total", "released_total", "spent_total", "inflight_total"):
                if ln[col] < 0:
                    problems.append(f"lane {ln['org']}/{ln['day']}/{ln['lane']} {col} 为负")
            if ln["reserved_total"] != ln["released_total"] + ln["spent_total"] + ln["inflight_total"]:
                problems.append(f"lane {ln['org']}/{ln['day']}/{ln['lane']} 恒等式破坏")
            net = ln["reserved_total"] - ln["released_total"]
            if net > ln["cap_units"]:
                problems.append(
                    f"lane {ln['org']}/{ln['day']}/{ln['lane']} 超份额: "
                    f"净占用 {net} > cap {ln['cap_units']}"
                )

        # c：车道求和 == 账户
        agg: dict[tuple[str, str], dict[str, int]] = {}
        for ln in lanes:
            slot = agg.setdefault(
                (ln["org"], ln["day"]),
                {"reserved_total": 0, "released_total": 0, "spent_total": 0, "inflight_total": 0},
            )
            for col in slot:
                slot[col] += ln[col]
        for a in accounts:
            got = agg.get((a["org"], a["day"]))
            if got is None or any(got[c] != a[c] for c in got):
                problems.append(f"account {a['org']}/{a['day']} 与车道汇总不一致: {got}")

        # d + e + g + h：逐预留行
        inflight_sum: dict[tuple[str, str], int] = {}
        lane_inflight: dict[tuple[str, str, str], int] = {}
        for r in con.execute("SELECT * FROM reservation").fetchall():
            key = (r["org"], r["day"])
            lkey = (r["org"], r["day"], r["lane"])
            if r["state"] in (STATE_RESERVED, STATE_HELD):
                if r["held_units"] != r["cost"]:
                    problems.append(f"reservation {r['request_id']} 在途金额不等于预留成本")
                inflight_sum[key] = inflight_sum.get(key, 0) + r["held_units"]
                lane_inflight[lkey] = lane_inflight.get(lkey, 0) + r["held_units"]
            else:
                if r["held_units"] != 0:
                    problems.append(f"reservation {r['request_id']} 已终结但 held_units={r['held_units']}")
            if r["state"] == STATE_SETTLED:
                if r["actual_cost"] is None or not (0 <= r["actual_cost"] <= r["cost"]):
                    problems.append(f"reservation {r['request_id']} 结算成本非法")
            # g：日界复核
            created = datetime.fromisoformat(r["created_at"])
            expect_day = self.policies.local_day(r["org"], created)
            if expect_day != r["day"]:
                problems.append(
                    f"reservation {r['request_id']} 配额日 {r['day']} 与机构时区"
                    f"计算结果 {expect_day} 不一致"
                )
            # h：事件序列与状态相容
            kinds = [
                e["event"]
                for e in con.execute(
                    "SELECT event FROM budget_event WHERE request_id=? ORDER BY id",
                    (r["request_id"],),
                ).fetchall()
            ]
            if not kinds or kinds[0] != policy_mod.EVENT_RESERVE:
                problems.append(f"reservation {r['request_id']} 缺少 RESERVE 首事件")
            if r["state"] == STATE_HELD and not (
                policy_mod.EVENT_HOLD in kinds or policy_mod.EVENT_REAP in kinds
            ):
                problems.append(f"reservation {r['request_id']} 状态 HELD 但无 HOLD/REAP 事件")
            if r["state"] == STATE_SETTLED and policy_mod.EVENT_SETTLE not in kinds:
                problems.append(f"reservation {r['request_id']} 状态 SETTLED 但无 SETTLE 事件")
            if r["state"] == STATE_REFUNDED and policy_mod.EVENT_REFUND not in kinds:
                problems.append(f"reservation {r['request_id']} 状态 REFUNDED 但无 REFUND 事件")

        for a in accounts:
            if inflight_sum.get((a["org"], a["day"]), 0) != a["inflight_total"]:
                problems.append(
                    f"account {a['org']}/{a['day']} 在途汇总 "
                    f"{inflight_sum.get((a['org'], a['day']), 0)} != inflight_total {a['inflight_total']}"
                )
        for ln in lanes:
            if lane_inflight.get((ln["org"], ln["day"], ln["lane"]), 0) != ln["inflight_total"]:
                problems.append(
                    f"lane {ln['org']}/{ln['day']}/{ln['lane']} 在途汇总与 inflight_total 不一致"
                )

        # f：事件流水重放聚合
        replay_agg: dict[tuple[str, str, str], dict[str, int]] = {}
        for e in con.execute("SELECT * FROM budget_event").fetchall():
            slot = replay_agg.setdefault(
                (e["org"], e["day"], e["lane"]),
                {"reserved": 0, "released": 0, "spent": 0},
            )
            if e["event"] == policy_mod.EVENT_RESERVE:
                slot["reserved"] += e["units"]
            elif e["event"] == policy_mod.EVENT_REFUND:
                slot["released"] += e["units"]
            elif e["event"] == policy_mod.EVENT_SETTLE:
                slot["spent"] += e["units"]
        for ln in lanes:
            got = replay_agg.get((ln["org"], ln["day"], ln["lane"]))
            if got is None:
                # 预创建但从未发生预留的空车道是合法的；有计数却无流水才是异常
                if any(ln[c] for c in ("reserved_total", "released_total", "spent_total")):
                    problems.append(f"lane {ln['org']}/{ln['day']}/{ln['lane']} 有计数但无事件流水")
                continue
            if got["reserved"] != ln["reserved_total"]:
                problems.append(
                    f"lane {ln['org']}/{ln['day']}/{ln['lane']} 重放预留 "
                    f"{got['reserved']} != {ln['reserved_total']}"
                )
            if got["released"] != ln["released_total"]:
                problems.append(
                    f"lane {ln['org']}/{ln['day']}/{ln['lane']} 重放释放 "
                    f"{got['released']} != {ln['released_total']}"
                )
            if got["spent"] != ln["spent_total"]:
                problems.append(
                    f"lane {ln['org']}/{ln['day']}/{ln['lane']} 重放消耗 "
                    f"{got['spent']} != {ln['spent_total']}"
                )
        return problems

    def assert_healthy(self) -> None:
        problems = self.verify()
        if problems:
            raise LedgerIntegrityError("; ".join(problems))


def open_ledger(
    db_path: str,
    policies: policy_mod.PolicyBook | None = None,
    *,
    now: Callable[[], datetime] = utc_now,
    lease_ttl_seconds: int = 300,
) -> BudgetLedger:
    return BudgetLedger(
        connect(db_path),
        policies or policy_mod.DEFAULT_POLICIES,
        now=now,
        lease_ttl_seconds=lease_ttl_seconds,
    )


# ---- 流水线阶段兼容（pipeline.py 仍按阶段名调用 process）----------------
def process(record):
    """记录变换阶段：预算账本由 scheduler/conditional 显式驱动，

    这里仅保留单一职责的字段标注，不产生任何额度副作用。"""
    rec = dict(record)
    rec.setdefault("_stage_budget", "ok")
    return rec
