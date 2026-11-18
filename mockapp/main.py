"""离线 mock server（fastapi）：回放 fixtures，供本地联调。"""
import json
from pathlib import Path
try:
    from fastapi import FastAPI
except Exception:  # pragma: no cover
    FastAPI = None

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "fixtures" / "input.json"
RESP_FIX = ROOT / "fixtures" / "responses.json"

if FastAPI is not None:
    app = FastAPI(title="mock")

    @app.get("/records")
    def records():
        return json.loads(FIX.read_text(encoding="utf-8"))

    @app.get("/responses")
    def responses():
        """多机构响应批次（错误包/空结果/乱序页/多 schema 版本）。"""
        return json.loads(RESP_FIX.read_text(encoding="utf-8"))
