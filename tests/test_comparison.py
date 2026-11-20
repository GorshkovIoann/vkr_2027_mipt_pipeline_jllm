import copy
import csv
import itertools
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from dagster import DagsterInstance

from examples.make_demo import make_demo
from sbs_comparison.core import (
    CRITERIA, OUTCOMES, combine, digest, normalize, overall, summarize, validate,
)
from sbs_comparison.definitions import SERVICES, _unique_object, build_job, comparison_job


def judgment(value):
    return {"status": "ok", "verdict": value} if value in ("left", "right", "tie") else {
        "status": value, "verdict": None}


class RulesTest(unittest.TestCase):
    def test_all_order_combinations(self):
        forward = {"left": "baseline", "right": "rival", "tie": "tie"}
        reverse = {"left": "rival", "right": "baseline", "tie": "tie"}
        for ab, ba in itertools.product(forward, repeat=2):
            with self.subTest(ab=ab, ba=ba):
                expected = forward[ab] if forward[ab] == reverse[ba] else "conflict"
                self.assertEqual(combine(judgment(ab), judgment(ba)), expected)

    def test_all_status_combinations(self):
        states = ("not_run", "invalid", "not_applicable", "insufficient", "left", "right", "tie")
        for a, b in itertools.product(states, repeat=2):
            if "not_run" in (a, b):
                expected = "not_run"
            elif "invalid" in (a, b):
                expected = "invalid"
            elif a == b == "not_applicable":
                expected = "not_applicable"
            elif "not_applicable" in (a, b):
                expected = "applicability_conflict"
            elif "insufficient" in (a, b):
                expected = "insufficient"
            else:
                continue  # Exhaustively covered above.
            with self.subTest(a=a, b=b):
                self.assertEqual(combine(judgment(a), judgment(b)), expected)

    def test_normalization(self):
        for verdict, order, expected in (("left", "ab", "baseline"), ("right", "ab", "rival"),
                                          ("left", "ba", "rival"), ("right", "ba", "baseline")):
            self.assertEqual(normalize(verdict, order), expected)
        with self.assertRaises(ValueError):
            normalize("left", "unknown")

    def test_overall_all_combinations(self):
        priority = ("not_run", "invalid", "applicability_conflict", "insufficient", "conflict")
        for values in itertools.product(OUTCOMES, repeat=3):
            if any(v in priority for v in values):
                expected = next(v for v in priority if v in values)
            elif set(values) == {"not_applicable"}:
                expected = "not_applicable"
            else:
                delta = values.count("baseline") - values.count("rival")
                expected = "baseline" if delta > 0 else "rival" if delta < 0 else "tie"
            self.assertEqual(overall(dict(zip(CRITERIA, values))), expected)

    def test_denominators(self):
        summary = summarize(["baseline", "rival", "tie", "not_applicable", "insufficient", "conflict"])
        self.assertEqual((summary["total"], summary["applicable"], summary["valid"]), (6, 5, 3))
        self.assertEqual(summary["coverage_all"], 0.5)
        self.assertEqual(summary["coverage_applicable"], 0.6)
        self.assertEqual(summary["rival_win_rate_valid"], 1 / 3)
        self.assertEqual(sum(summary["counts"].values()), 6)

    def test_empty_denominators(self):
        for values in ([], ["not_applicable"], ["insufficient"]):
            self.assertIsNone(summarize(values)["rival_win_rate_valid"])
        self.assertIsNone(summarize(["not_applicable"])["coverage_applicable"])

    def test_unknown_outcome(self):
        with self.assertRaises(ValueError):
            summarize(["unknown"])
        with self.assertRaises(ValueError):
            overall({"solved": "tie"})


class ValidationTest(unittest.TestCase):
    def test_demo_valid(self):
        data = make_demo()
        self.assertIs(validate(data, SERVICES), data)
        self.assertEqual(len(data["judgments"]), len(data["pairs"]) * 6)

    def test_invalid_inputs(self):
        mutations = [
            lambda d: d["pairs"].append(d["pairs"][0]),
            lambda d: d["judgments"].append(d["judgments"][0]),
            lambda d: d["judgments"].pop(),
            lambda d: d["judgments"][0].update(order="wrong"),
            lambda d: d["judgments"][0].update(criterion="wrong"),
            lambda d: d["judgments"][0].update(status="wrong"),
            lambda d: d["judgments"][0].update(verdict="wrong"),
            lambda d: d["judgments"][0].update(status="insufficient"),
            lambda d: d["judgments"][0].update(case_id="unknown"),
            lambda d: d["pairs"][0].update(service="unknown"),
            lambda d: d["pairs"][0].update(case_id=" "),
            lambda d: d.update(synthetic="false"),
            lambda d: d.update(schema_version=True),
            lambda d: d.update(pairs=[]),
            lambda d: d.update(unexpected="field"),
            lambda d: d["pairs"][0].update(query="unexpected text"),
            lambda d: d.update(judgments={}),
        ]
        for i, mutate in enumerate(mutations):
            data = make_demo()
            mutate(data)
            with self.subTest(mutation=i), self.assertRaises(ValueError):
                validate(data, SERVICES)

    def test_unequal_service_sizes_allowed(self):
        data = make_demo()
        data["pairs"] = [p for p in data["pairs"] if (p["case_id"], p["service"]) != ("mixed", "rival_3")]
        data["judgments"] = [r for r in data["judgments"] if (r["case_id"], r["service"]) != ("mixed", "rival_3")]
        validate(data, SERVICES)

    def test_duplicate_json_keys(self):
        with self.assertRaises(ValueError):
            json.loads('{"a": 1, "a": 2}', object_pairs_hook=_unique_object)

    def test_invalid_services(self):
        for services in ((), ("a", "a"), ("../x",), (3,), ("UPPER",)):
            with self.subTest(services=services), self.assertRaises(ValueError):
                build_job(services)


class GraphTest(unittest.TestCase):
    def run_graph(self, root, data, raise_on_error=True):
        source = root / "input.json"
        source.write_text(json.dumps(data), encoding="utf-8")
        with DagsterInstance.ephemeral() as instance:
            with patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden")), \
                 patch.object(socket, "getaddrinfo", side_effect=AssertionError("DNS forbidden")):
                return comparison_job.execute_in_process(
                    instance=instance, raise_on_error=raise_on_error,
                    run_config={"ops": {"load_input": {"config": {"input_path": str(source)}},
                                        "publish": {"config": {"output_dir": str(root / "results")}}},
                                "loggers": {"console": {"config": {"log_level": "ERROR"}}}})

    def test_topology(self):
        names = {node.name for node in comparison_job.graph.node_defs}
        self.assertEqual(len(names), 37)
        self.assertEqual(sum("_replay_" in name for name in names), 18)
        self.assertEqual(sum(name.endswith("_reconcile") for name in names), 9)
        self.assertEqual(sum(name.endswith("_aggregate") for name in names), 3)
        self.assertEqual(len(build_job(("only_rival",)).graph.node_defs), 17)

    def test_full_run_offline_and_reproducible(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, data = Path(tmp), make_demo()
            original = copy.deepcopy(data)
            first = self.run_graph(root, data)
            second = self.run_graph(root, data)
            self.assertTrue(first.success and second.success)
            self.assertEqual(len([e for e in first.all_events if e.is_step_success]), 37)
            output = Path(first.output_for_node("publish"))
            other = Path(second.output_for_node("publish"))
            self.assertNotEqual(output, other)
            self.assertEqual((output / "report.json").read_bytes(), (other / "report.json").read_bytes())
            self.assertEqual((output / "outcomes.csv").read_bytes(), (other / "outcomes.csv").read_bytes())
            report = json.loads((output / "report.json").read_text())
            self.assertTrue(report["synthetic"])
            self.assertEqual(report["input_sha256"], digest(original))
            for service in SERVICES:
                stats = report["services"][service]["metrics"]["overall"]
                self.assertEqual((stats["total"], stats["valid"]), (10, 3))
                self.assertEqual(stats["counts"]["insufficient"], 2)
            with (output / "outcomes.csv").open(newline="") as stream:
                self.assertEqual(len(list(csv.DictReader(stream))), 30)
            self.assertEqual(json.loads((root / "input.json").read_text()), original)

    def test_bad_input_never_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, data = Path(tmp), make_demo()
            data["judgments"].pop()
            result = self.run_graph(root, data, raise_on_error=False)
            self.assertFalse(result.success)
            self.assertFalse((root / "results").exists())

    def test_csv_formula_escaped(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = make_demo()
            for row in data["pairs"] + data["judgments"]:
                if row["case_id"] == "mixed":
                    row["case_id"] = "=1+1"
            result = self.run_graph(Path(tmp), data)
            output = Path(result.output_for_node("publish"))
            with (output / "outcomes.csv").open(newline="") as stream:
                ids = [r["case_id"] for r in csv.DictReader(stream)]
            self.assertIn("'=1+1", ids)
            self.assertNotIn("=1+1", ids)


if __name__ == "__main__":
    unittest.main()
