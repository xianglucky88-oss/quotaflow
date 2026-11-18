"""单一职责：从本地 fixtures 读取原始数据（离线摄取入口，不联网）。

* ``load()``：旧入口，读取裸记录列表（逐记录流水线）。
* ``load_responses()``：新入口，读取多机构响应信封批次
  （错误包/空结果/乱序页/多 schema 版本），供 normalize_response/batch 消费。
"""
import json
from .config import FIXTURES, RESPONSES_FIXTURE


def load():
    with open(FIXTURES, encoding="utf-8") as f:
        return json.load(f)


def load_responses(path=None):
    target = path or RESPONSES_FIXTURE
    with open(target, encoding="utf-8") as f:
        return json.load(f)
