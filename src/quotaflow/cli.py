import json, sys
from . import pipeline


def main():
    # `python -m quotaflow.cli sync` 走多机构响应规范化同步；默认保持旧裸记录流水线。
    if len(sys.argv) > 1 and sys.argv[1] == "sync":
        print(json.dumps(pipeline.sync_responses(), ensure_ascii=False))
    else:
        print(json.dumps(pipeline.run(), ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
