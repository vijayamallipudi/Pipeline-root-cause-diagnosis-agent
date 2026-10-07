"""Runs all checks and works out the root cause.

The idea: problems cascade downstream, so the root cause is the *earliest*
failure in pipeline order. Everything after it is treated as a symptom.
When two failures show up at the same transition, the more specific one
wins (a schema change explains nulls, a key change explains row loss, ...).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

from pipeline_rca import config
from pipeline_rca.db import connect
from pipeline_rca.diagnostics.checks import ALL_CHECKS, Finding
from pipeline_rca.diagnostics.contracts import TRANSITIONS
from pipeline_rca.diagnostics.snapshot import save_snapshots, take_snapshots

# lower = more likely to be the actual cause rather than a side effect
CATEGORY_PRIORITY = {
    "SCHEMA_DRIFT": 0,
    "JOIN_KEY_MISMATCH": 1,
    "DUPLICATE_ROWS": 2,
    "NULL_SPIKE": 3,
    "ROW_INFLATION": 4,
    "ROW_LOSS": 5,
    "MEASURE_DRIFT": 6,
    "DISTRIBUTION_SHIFT": 7,
}

# used to score the diagnosis against what the chaos injector actually did
CATEGORY_TO_SCENARIO = {
    "SCHEMA_DRIFT": "type_change",
    "NULL_SPIKE": "null_spike",
    "JOIN_KEY_MISMATCH": "dropped_join_key",
    "DUPLICATE_ROWS": "duplicate_rows",
}
STAGE_TO_TRANSITION = {"extract": "source->raw", "staging": "raw->staging"}

REPORTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.META}.diagnostic_reports (
    run_id       VARCHAR,
    created_at   TIMESTAMP,
    status       VARCHAR,
    root_cause   VARCHAR,
    report       JSON
)
"""


@dataclass
class DiagnosticReport:
    run_id: str | None
    status: str  # HEALTHY / FAILED
    root_cause: Finding | None
    symptoms: list[Finding]
    expected_changes: list[Finding]
    checks: dict[str, str]
    layers: dict[str, dict]
    impact: dict
    evaluation: dict = field(default_factory=dict)

    @property
    def failures(self) -> list[Finding]:
        return ([self.root_cause] if self.root_cause else []) + self.symptoms

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "root_cause": self.root_cause.to_dict() if self.root_cause else None,
            "symptoms": [f.to_dict() for f in self.symptoms],
            "expected_changes": [f.to_dict() for f in self.expected_changes],
            "checks": self.checks,
            "layers": self.layers,
            "impact": self.impact,
            "evaluation": self.evaluation,
        }

    def for_llm(self) -> dict:
        """Same as to_dict but without the answer key - the LLM shouldn't see ground truth."""
        d = self.to_dict()
        d.pop("evaluation")
        return d


def _sort_key(f: Finding) -> tuple:
    return (TRANSITIONS.index(f.transition), CATEGORY_PRIORITY.get(f.category, 99))


def _impact(snaps) -> dict:
    stg, fin = snaps["staging.loans"], snaps["final.loan_portfolio"]
    return {
        "final_loan_rows": fin.row_count,
        "final_distinct_loans": fin.row_count - fin.duplicate_keys,
        "final_total_balance": fin.columns["balance"].sum,
        "staging_loan_rows": stg.row_count,
    }


def _evaluate(root: Finding | None, ground_truth: dict) -> dict:
    """Did we find what the chaos injector broke?"""
    if not ground_truth:
        return {"scenario": "none", "expected": "HEALTHY", "correct": root is None}
    expected_transition = STAGE_TO_TRANSITION[ground_truth["injected_at"].split(":")[0]]
    expected_table = ground_truth["injected_at"].replace("extract:", "raw.").replace("staging:", "staging.")
    got_scenario = CATEGORY_TO_SCENARIO.get(root.category) if root else None
    return {
        "scenario": ground_truth["scenario"],
        "diagnosed_as": got_scenario,
        "scenario_correct": got_scenario == ground_truth["scenario"],
        "location_expected": f"{expected_transition} {expected_table}",
        "location_found": f"{root.transition} {root.table}" if root else None,
        "location_correct": bool(root) and root.transition == expected_transition and root.table == expected_table,
        "correct": got_scenario == ground_truth["scenario"]
        and bool(root) and root.transition == expected_transition and root.table == expected_table,
    }


def _find_run(con, run_id: str | None) -> tuple[str | None, dict]:
    """The given run, or the most recent one if no id is passed."""
    sql = f"SELECT run_id, ground_truth FROM {config.META}.pipeline_runs "
    sql += "WHERE run_id = ?" if run_id else "ORDER BY started_at DESC LIMIT 1"
    try:
        row = con.execute(sql, [run_id] if run_id else []).fetchone()
    except duckdb.CatalogException:  # no runs table yet
        return run_id, {}
    if not row:
        return run_id, {}
    return row[0], json.loads(row[1]) if row[1] else {}


def diagnose(db_path=None, run_id: str | None = None, save: bool = True) -> DiagnosticReport:
    with connect(db_path) as con:
        run_id, ground_truth = _find_run(con, run_id)

        snaps = take_snapshots(con)
        findings: list[Finding] = []
        checks: dict[str, str] = {}
        for check in ALL_CHECKS:
            out = check(con, snaps)
            findings += out
            checks[check.__name__.removeprefix("check_")] = "FAIL" if any(f.failed for f in out) else "PASS"

        failures = sorted((f for f in findings if f.failed), key=_sort_key)
        root = failures[0] if failures else None

        report = DiagnosticReport(
            run_id=run_id,
            status="FAILED" if root else "HEALTHY",
            root_cause=root,
            symptoms=failures[1:],
            expected_changes=[f for f in findings if not f.failed],
            checks=checks,
            layers={name: {"rows": s.row_count, "duplicate_keys": s.duplicate_keys} for name, s in snaps.items()},
            impact=_impact(snaps),
            evaluation=_evaluate(root, ground_truth),
        )

        if save and run_id:
            save_snapshots(con, run_id, snaps)
            con.execute(REPORTS_DDL)
            con.execute(f"DELETE FROM {config.META}.diagnostic_reports WHERE run_id = ?", [run_id])
            con.execute(
                f"INSERT INTO {config.META}.diagnostic_reports VALUES (?, ?, ?, ?, ?)",
                [run_id, datetime.now(timezone.utc).replace(tzinfo=None), report.status,
                 root.category if root else None, json.dumps(report.to_dict(), default=str)],
            )
    return report


def load_report(run_id: str, db_path=None) -> DiagnosticReport:
    """Rebuild a saved report - lets a later task pick up where diagnose() left off."""
    with connect(db_path) as con:
        row = con.execute(
            f"SELECT report FROM {config.META}.diagnostic_reports WHERE run_id = ?", [run_id]
        ).fetchone()
    if row is None:
        raise KeyError(f"no diagnostic report saved for run {run_id}")
    d = json.loads(row[0])
    as_finding = lambda f: Finding(**f)  # noqa: E731
    return DiagnosticReport(
        run_id=d["run_id"],
        status=d["status"],
        root_cause=as_finding(d["root_cause"]) if d["root_cause"] else None,
        symptoms=[as_finding(f) for f in d["symptoms"]],
        expected_changes=[as_finding(f) for f in d["expected_changes"]],
        checks=d["checks"],
        layers=d["layers"],
        impact=d["impact"],
        evaluation=d.get("evaluation", {}),
    )
