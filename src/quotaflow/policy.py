"""机构预算策略（纯策略模块，不做任何 IO）。

本模块集中回答两个需要"依据"的问题，其余模块只引用结论、不各自决策：

1. 跨日重置采用哪个机构时区？
   每个机构配置一个 IANA 时区名（如 ``Asia/Shanghai``、``America/Los_Angeles``）。
   配额日的分界线取该时区的本地午夜 00:00。
   依据：
     - IANA 时区名携带完整夏令时规则，固定 UTC 偏移无法表达 DST，跨 DST 的重置点会漂移；
     - 多机构 API 配额普遍按"机构所在地营业日"重置，机构配置自身时区即归属该营业日；
     - 未配置时区时回退 UTC（保守默认），并由加载方显式告警，避免静默按错误时区重置。

2. 未知请求结果何时可以退款？
   只有拿到"远端未消耗或已退还额度"的凭据时才允许退款；超时 / 5xx / 连接错误 / 408
   属于结果未知，一律不得释放额度，只能转为 HELD 挂起，等待条件重试或人工对账。
   依据：
     - RFC 9110：429 表示服务端拒绝处理、304 表示条件命中未返回正文，语义上均未产生全量消耗；
     - 幂等性原则：结果未知时释放额度，重试请求将失去额度约束，可能造成真实超额；
     - 每条退款理由都声明必需凭据字段，凭据不全在账本层直接拒绝（RefundNotAllowed）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "UTC"

# ---- 预留状态机 ---------------------------------------------------------
# RESERVED : 已预留、租约有效，请求在途
# HELD     : 结果未知（超时/5xx/租约过期被回收），额度继续挂账，等待对账
# SETTLED  : 已按实际成本结算（终态）
# REFUNDED : 凭据齐全的全额退款（终态）
STATE_RESERVED = "RESERVED"
STATE_HELD = "HELD"
STATE_SETTLED = "SETTLED"
STATE_REFUNDED = "REFUNDED"
TERMINAL_STATES = frozenset({STATE_SETTLED, STATE_REFUNDED})

# ---- 事件流水种类（budget_event.event 的 CHECK 约束同步维护）-------------
EVENT_RESERVE = "RESERVE"
EVENT_SETTLE = "SETTLE"
EVENT_REFUND = "REFUND"
EVENT_HOLD = "HOLD"
EVENT_REAP = "REAP"
EVENT_KINDS = frozenset(
    {EVENT_RESERVE, EVENT_SETTLE, EVENT_REFUND, EVENT_HOLD, EVENT_REAP}
)

# ---- 允许退款的理由及其必需凭据 -----------------------------------------
# 理由 -> 凭据中必须出现的字段；不在此表中的理由（尤其 unknown/timeout）
# 永远无法通过账本的退款校验。
REFUND_EVIDENCE: dict[str, frozenset[str]] = {
    # RFC 9110 15.5.5.2.6：429 表示服务端拒绝提供服务，配额未被消耗
    "rate_limited": frozenset({"status", "retry_after"}),
    # 4xx（408/429 除外）表示请求被拒、服务端未处理
    "client_rejected": frozenset({"status"}),
    # 用幂等键向服务端反查，确认该请求从未被受理
    "reconciled_not_served": frozenset({"lookup_request_id", "lookup_status"}),
}

# 结果未知、只能挂起的理由
HOLD_REASONS = frozenset(
    {"timeout", "http_408", "http_5xx", "connection_error", "lease_expired", "manual"}
)

# 条件命中（304）的结算理由
SETTLE_NOT_MODIFIED = "not_modified"
SETTLE_PARTIAL = "partial"
SETTLE_FULL = "full"


@dataclass(frozen=True)
class LaneSpec:
    """一个接口优先级车道。

    weight 同时承担两个含义：
      - 优先级：weight 越大调度越优先；
      - 公平份额：车道日上限 cap = 按 weight 切分机构日限额得到的保证份额。
    """

    name: str
    weight: int

    def __post_init__(self) -> None:
        if self.weight <= 0:
            raise ValueError(f"lane weight must be positive: {self.name}")


@dataclass(frozen=True)
class OrgPolicy:
    org: str
    timezone: str
    daily_limit: int
    lanes: tuple[LaneSpec, ...]
    # 条件请求（304）的成本，必须不高于一次全量请求成本
    conditional_cost: int = 1

    def __post_init__(self) -> None:
        if self.daily_limit <= 0:
            raise ValueError(f"daily_limit must be positive: {self.org}")
        if not self.lanes:
            raise ValueError(f"at least one lane required: {self.org}")
        # 立即校验时区名合法（非法 IANA 名在首次使用时才会报错，提前失败）
        ZoneInfo(self.timezone)
        if self.conditional_cost < 0:
            raise ValueError("conditional_cost must be >= 0")

    def lane(self, name: str) -> LaneSpec:
        for spec in self.lanes:
            if spec.name == name:
                return spec
        raise KeyError(f"unknown lane {name!r} for org {self.org!r}")

    def lane_caps(self) -> dict[str, int]:
        """按权重把日限额切分为各车道的保证份额（账本层硬约束）。

        cap_i = floor(limit * w_i / W)；取整余量按 weight 降序、车道名升序
        逐车道补发 1，保证总和恰好等于 daily_limit，且结果确定可复算。
        余量不跨车道挪用：优先保证任何车道都无法挤占别人的份额，
        代价是闲置份额保持闲置——以可预测性换取绝不超额。
        """
        total = sum(s.weight for s in self.lanes)
        ordered = sorted(self.lanes, key=lambda s: (-s.weight, s.name))
        caps = {s.name: (self.daily_limit * s.weight) // total for s in ordered}
        remainder = self.daily_limit - sum(caps.values())
        for spec in ordered:
            if remainder == 0:
                break
            caps[spec.name] += 1
            remainder -= 1
        return caps


@dataclass
class PolicyBook:
    """机构策略集合，账本与调度器只通过它读取策略。"""

    orgs: dict[str, OrgPolicy] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: dict) -> "PolicyBook":
        orgs: dict[str, OrgPolicy] = {}
        for org, cfg in raw.items():
            tz = cfg.get("timezone") or DEFAULT_TIMEZONE
            lanes = tuple(
                LaneSpec(name=item["name"], weight=int(item["weight"]))
                for item in cfg["lanes"]
            )
            orgs[org] = OrgPolicy(
                org=org,
                timezone=tz,
                daily_limit=int(cfg["daily_limit"]),
                lanes=lanes,
                conditional_cost=int(cfg.get("conditional_cost", 1)),
            )
        return cls(orgs=orgs)

    def get(self, org: str) -> OrgPolicy:
        try:
            return self.orgs[org]
        except KeyError:
            raise KeyError(f"no policy configured for org {org!r}") from None

    def local_day(self, org: str, instant) -> str:
        """返回机构时区下 instant 所属的配额日（YYYY-MM-DD）。"""
        from datetime import datetime

        tz = ZoneInfo(self.get(org).timezone)
        if not isinstance(instant, datetime):
            raise TypeError("instant must be a timezone-aware datetime")
        if instant.tzinfo is None:
            raise ValueError("instant must be timezone-aware")
        return instant.astimezone(tz).date().isoformat()


# ---- 演示/测试用默认策略册（离线流程与测试共用）-------------------------
DEFAULT_POLICIES = PolicyBook.from_mapping(
    {
        "alpha": {
            "timezone": "Asia/Shanghai",
            "daily_limit": 100,
            "lanes": [
                {"name": "high", "weight": 3},
                {"name": "low", "weight": 1},
            ],
            "conditional_cost": 1,
        },
        "beta": {
            "timezone": "America/Los_Angeles",
            "daily_limit": 60,
            "lanes": [
                {"name": "high", "weight": 2},
                {"name": "low", "weight": 1},
            ],
            "conditional_cost": 1,
        },
    }
)
