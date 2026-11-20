"""Create only synthetic verdicts, not queries, answers or model evaluations."""

import json
from pathlib import Path

CRITERIA = ("solved", "completeness", "geo")
# Each entry is (AB, BA), with baseline first in AB and rival first in BA.
PATTERNS = {
    "baseline_wins": [("left", "right")] * 3,
    "rival_wins": [("right", "left")] * 3,
    "tie": [("tie", "tie")] * 3,
    "order_conflict": [("left", "left")] * 3,
    "insufficient": [("insufficient", "right")] * 3,
    "not_applicable": [("not_applicable", "not_applicable")] * 3,
    "applicability_conflict": [("not_applicable", "left")] * 3,
    "invalid": [("invalid", "left")] * 3,
    "not_run": [("not_run", "left")] * 3,
    "mixed": [("right", "left"), ("right", "left"), ("insufficient", "left")],
}


def make_demo():
    pairs, judgments = [], []
    for service in ("rival_1", "rival_2", "rival_3"):
        for case, patterns in PATTERNS.items():
            pairs.append({"case_id": case, "service": service})
            for criterion, pattern in zip(CRITERIA, patterns):
                for order, value in zip(("ab", "ba"), pattern):
                    ok = value in ("left", "right", "tie")
                    judgments.append({"case_id": case, "service": service, "criterion": criterion,
                                      "order": order, "status": "ok" if ok else value,
                                      "verdict": value if ok else None})
    return {"schema_version": 1, "synthetic": True, "pairs": pairs, "judgments": judgments}


if __name__ == "__main__":
    path = Path("artifacts/demo.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(make_demo(), indent=2) + "\n", encoding="utf-8")
    print(path)
