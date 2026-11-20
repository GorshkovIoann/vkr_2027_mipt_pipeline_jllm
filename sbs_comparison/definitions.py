"""Executable replay graph: 4 preparation + 10 per service + 3 final ops."""

import csv
import json
from pathlib import Path
import re

from dagster import Definitions, Field, RetryPolicy, in_process_executor, job, mem_io_manager, op

from .core import CRITERIA, combine, digest, metrics, overall, validate

SERVICES = ("rival_1", "rival_2", "rival_3")
NO_RETRIES = RetryPolicy(max_retries=0)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


@op(config_schema={"input_path": str}, retry_policy=NO_RETRIES)
def load_input(context) -> dict:
    return json.loads(Path(context.op_config["input_path"]).read_text(encoding="utf-8"),
                      object_pairs_hook=_unique_object)


@op(retry_policy=NO_RETRIES)
def index_judgments(data: dict) -> dict:
    return {**data, "index": {(r["case_id"], r["service"], r["criterion"], r["order"]): r
                              for r in data["judgments"]}}


@op(retry_policy=NO_RETRIES)
def fingerprint_input(data: dict) -> dict:
    original = {key: value for key, value in data.items() if key != "index"}
    return {**data, "input_sha256": digest(original)}


def replay_op(service, criterion, order):
    @op(name=f"{service}_{criterion}_replay_{order}", retry_policy=NO_RETRIES,
        description="Read saved judgments; no LLM calls.")
    def replay(data: dict) -> dict:
        return {p["case_id"]: data["index"][(p["case_id"], service, criterion, order)]
                for p in data["pairs"] if p["service"] == service}
    return replay


def reconcile_op(service, criterion):
    @op(name=f"{service}_{criterion}_reconcile", retry_policy=NO_RETRIES)
    def reconcile(ab: dict, ba: dict) -> dict:
        if ab.keys() != ba.keys():
            raise ValueError("AB/BA case mismatch")
        return {case: combine(ab[case], ba[case]) for case in sorted(ab)}
    return reconcile


def aggregate_op(service):
    @op(name=f"{service}_aggregate", retry_policy=NO_RETRIES)
    def aggregate(solved: dict, completeness: dict, geo: dict) -> dict:
        if not solved.keys() == completeness.keys() == geo.keys():
            raise ValueError("Criterion case mismatch")
        rows = []
        for case in sorted(solved):
            criteria = {"solved": solved[case], "completeness": completeness[case], "geo": geo[case]}
            rows.append({"case_id": case, "service": service, "criteria": criteria, "overall": overall(criteria)})
        return {"service": service, "rows": rows, "metrics": metrics(rows)}
    return aggregate


@op(retry_policy=NO_RETRIES)
def compare_all(results: list, data: dict) -> dict:
    return {"schema_version": 1, "mode": "saved_judgments_replay", "synthetic": data["synthetic"],
            "policy": "strict-equal-weight-v1", "input_sha256": data["input_sha256"],
            "network_calls": 0, "services": {r["service"]: r for r in results}}


@op(retry_policy=NO_RETRIES)
def verify_report(report: dict, data: dict) -> dict:
    expected = {}
    for pair in data["pairs"]:
        case, service = pair["case_id"], pair["service"]
        criteria = {c: combine(data["index"][(case, service, c, "ab")],
                               data["index"][(case, service, c, "ba")]) for c in CRITERIA}
        expected.setdefault(service, []).append({"case_id": case, "service": service,
                                                 "criteria": criteria, "overall": overall(criteria)})
    if report["services"].keys() != expected.keys():
        raise ValueError("Report service mismatch")
    for service, rows in expected.items():
        rows.sort(key=lambda row: row["case_id"])
        if report["services"][service] != {"service": service, "rows": rows, "metrics": metrics(rows)}:
            raise ValueError("Report verification failed")
    return report


@op(config_schema={"output_dir": Field(str, default_value="artifacts")}, retry_policy=NO_RETRIES)
def publish(context, report: dict) -> str:
    # A new directory per run: never overwrite source data or previous reports.
    directory = Path(context.op_config["output_dir"]) / context.run_id
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    with (directory / "outcomes.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("case_id", "service", *CRITERIA, "overall"))
        writer.writeheader()
        for result in report["services"].values():
            for row in result["rows"]:
                # JSON is the canonical export; CSV cells are escaped for spreadsheets.
                flat = {"case_id": row["case_id"], "service": row["service"],
                        **row["criteria"], "overall": row["overall"]}
                writer.writerow({k: "'" + v if v.startswith(("=", "+", "-", "@", "\t", "\r", "\n")) else v
                                 for k, v in flat.items()})
    context.add_output_metadata({"report_sha256": digest(report), "services": len(report["services"])})
    return str(directory)


def build_job(services=SERVICES):
    services = tuple(services)
    if (not services or any(not isinstance(s, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", s) for s in services)
            or len(set(services)) != len(services)):
        raise ValueError("Services must be unique lowercase identifiers")

    @op(name="validate_input", retry_policy=NO_RETRIES)
    def validate_input(data: dict) -> dict:
        return validate(data, services)

    @job(name="compare_saved_judgments", executor_def=in_process_executor,
         resource_defs={"io_manager": mem_io_manager},
         description="Offline comparison; AB = baseline first, BA = rival first.")
    def comparison():
        data = fingerprint_input(index_judgments(validate_input(load_input())))
        results = []
        for service in services:
            reconciled = []
            for criterion in CRITERIA:
                ab = replay_op(service, criterion, "ab")(data)
                ba = replay_op(service, criterion, "ba")(data)
                reconciled.append(reconcile_op(service, criterion)(ab, ba))
            results.append(aggregate_op(service)(*reconciled))
        publish(verify_report(compare_all(results, data), data))
    return comparison


comparison_job = build_job()
defs = Definitions(jobs=[comparison_job])
