from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"
RESPONSES_FIXTURE = ROOT / "fixtures" / "responses.json"
DB_PATH = ROOT / "quotaflow.db"
