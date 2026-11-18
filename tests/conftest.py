"""测试夹具：所有落库/归档都隔离在临时 sqlite，避免污染项目 quotaflow.db。"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from quotaflow import config  # noqa: E402


@pytest.fixture()
def temp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_quotaflow.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    return db_path
