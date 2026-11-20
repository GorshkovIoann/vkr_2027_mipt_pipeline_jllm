"""Pure comparison rules, independent of Dagster and model providers."""

from collections import Counter
import hashlib
import json

CRITERIA = ("solved", "completeness", "geo")
ORDERS = ("ab", "ba")
UNRESOLVED = ("not_run", "invalid", "applicability_conflict", "insufficient", "conflict")
OUTCOMES = ("baseline", "rival", "tie", "not_applicable", *UNRESOLVED)
INPUT_STATUSES = ("ok", "not_applicable", "insufficient", "invalid", "not_run")


def digest(value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _fields(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError("Unexpected object fields")


def validate(data, services):
    """Reject duplicates, missing evaluations, extra fields and ambiguous verdicts."""
    _fields(data, ("schema_version", "synthetic", "pairs", "judgments"))
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("Unsupported schema version")
    if type(data["synthetic"]) is not bool:
        raise ValueError("synthetic must be a boolean")
    if not isinstance(data["pairs"], list) or not data["pairs"]:
        raise ValueError("pairs must be a nonempty list")
    pairs = set()
    for pair in data["pairs"]:
        _fields(pair, ("case_id", "service"))
        if not isinstance(pair["case_id"], str) or not pair["case_id"].strip():
            raise ValueError("case_id must be a nonempty string")
        if not isinstance(pair["service"], str) or pair["service"] not in services:
            raise ValueError("Unknown service")
        key = (pair["case_id"], pair["service"])
        if key in pairs:
            raise ValueError("Duplicate pair")
        pairs.add(key)
    if {service for _, service in pairs} != set(services):
        raise ValueError("Every configured service needs at least one pair")
    if not isinstance(data["judgments"], list):
        raise ValueError("judgments must be a list")
    seen = set()
    for row in data["judgments"]:
        _fields(row, ("case_id", "service", "criterion", "order", "status", "verdict"))
        if any(not isinstance(row[k], str) for k in ("case_id", "service", "criterion", "order", "status")):
            raise ValueError("Judgment identifiers and status must be strings")
        if row["criterion"] not in CRITERIA or row["order"] not in ORDERS:
            raise ValueError("Unknown criterion or order")
        if row["status"] not in INPUT_STATUSES:
            raise ValueError("Unknown judgment status")
        if row["status"] == "ok":
            if row["verdict"] not in ("left", "right", "tie"):
                raise ValueError("ok requires left, right or tie")
        elif row["verdict"] is not None:
            raise ValueError("Non-ok judgments require a null verdict")
        key = (row["case_id"], row["service"], row["criterion"], row["order"])
        if key in seen:
            raise ValueError("Duplicate judgment")
        seen.add(key)
    expected = {(case, service, c, o) for case, service in pairs for c in CRITERIA for o in ORDERS}
    if seen != expected:
        raise ValueError("Judgment keys do not match pairs; use explicit not_run for missing evaluations")
    return data


def normalize(verdict, order):
    if order not in ORDERS or verdict not in ("left", "right", "tie"):
        raise ValueError("Invalid verdict or order")
    if verdict == "tie":
        return "tie"
    return "baseline" if (verdict == "left") == (order == "ab") else "rival"


def combine(ab, ba):
    statuses = (ab["status"], ba["status"])
    for status in ("not_run", "invalid"):
        if status in statuses:
            return status
    if statuses == ("not_applicable", "not_applicable"):
        return "not_applicable"
    if "not_applicable" in statuses:
        return "applicability_conflict"
    if "insufficient" in statuses:
        return "insufficient"
    first, second = normalize(ab["verdict"], "ab"), normalize(ba["verdict"], "ba")
    return first if first == second else "conflict"


def overall(criteria):
    """Equal-weight signed sum, only when every applicable criterion is resolved."""
    if set(criteria) != set(CRITERIA) or any(v not in OUTCOMES for v in criteria.values()):
        raise ValueError("Invalid criterion outcomes")
    values = list(criteria.values())
    for status in UNRESOLVED:
        if status in values:
            return status
    applicable = [value for value in values if value != "not_applicable"]
    if not applicable:
        return "not_applicable"
    score = sum({"baseline": 1, "rival": -1, "tie": 0}[v] for v in applicable)
    return "baseline" if score > 0 else "rival" if score < 0 else "tie"


def summarize(values):
    counts = Counter(values)
    if set(counts) - set(OUTCOMES):
        raise ValueError("Unknown outcome")
    total = sum(counts.values())
    valid = sum(counts[k] for k in ("baseline", "rival", "tie"))
    applicable = total - counts["not_applicable"]
    return {
        "total": total, "applicable": applicable, "valid": valid,
        "counts": {k: counts[k] for k in OUTCOMES},
        "coverage_all": valid / total if total else None,
        "coverage_applicable": valid / applicable if applicable else None,
        "baseline_win_rate_valid": counts["baseline"] / valid if valid else None,
        "rival_win_rate_valid": counts["rival"] / valid if valid else None,
        "tie_rate_valid": counts["tie"] / valid if valid else None,
    }


def metrics(rows):
    return {c: summarize(row["criteria"][c] if c != "overall" else row["overall"] for row in rows)
            for c in (*CRITERIA, "overall")}
