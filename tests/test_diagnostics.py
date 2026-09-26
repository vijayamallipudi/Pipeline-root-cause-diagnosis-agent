"""The diagnosis should name the right problem at the right place for every scenario."""

import pytest

from pipeline_rca.chaos import Scenario
from pipeline_rca.db import connect
from pipeline_rca.diagnostics import diagnose
from pipeline_rca.pipeline import run_pipeline


def run_and_diagnose(tmp_path, scenario):
    db = tmp_path / "diag.duckdb"
    result = run_pipeline(scenario, db_path=db)
    return db, result, diagnose(db_path=db, run_id=result.run_id)


def test_healthy_run_has_no_failures(tmp_path):
    _, _, report = run_and_diagnose(tmp_path, Scenario.NONE)
    assert report.status == "HEALTHY"
    assert report.failures == []
    assert all(v == "PASS" for v in report.checks.values())
    # the WITHDRAWN drop is reported, but as expected - not a failure
    assert any("WITHDRAWN" in f.message for f in report.expected_changes)


@pytest.mark.parametrize(
    "scenario, category, transition, table",
    [
        (Scenario.TYPE_CHANGE, "SCHEMA_DRIFT", "source->raw", "raw.loans"),
        (Scenario.NULL_SPIKE, "NULL_SPIKE", "source->raw", "raw.customers"),
        (Scenario.DROPPED_JOIN_KEY, "JOIN_KEY_MISMATCH", "raw->staging", "staging.loans"),
        (Scenario.DUPLICATE_ROWS, "DUPLICATE_ROWS", "raw->staging", "staging.loans"),
    ],
)
def test_root_cause_found(tmp_path, scenario, category, transition, table):
    _, _, report = run_and_diagnose(tmp_path, scenario)
    rc = report.root_cause
    assert report.status == "FAILED"
    assert (rc.category, rc.transition, rc.table) == (category, transition, table)
    assert report.evaluation["correct"]


def test_type_change_evidence(tmp_path):
    _, _, report = run_and_diagnose(tmp_path, Scenario.TYPE_CHANGE)
    ev = report.root_cause.evidence
    assert ev["affected_segment"] == "source_system = 'CORE_BANKING'"
    assert "currency" in ev["pattern"]
    assert {f.category for f in report.symptoms} >= {"NULL_SPIKE", "MEASURE_DRIFT"}


def test_dropped_join_key_evidence(tmp_path):
    _, result, report = run_and_diagnose(tmp_path, Scenario.DROPPED_JOIN_KEY)
    ev = report.root_cause.evidence
    assert ev["affected_segment"] == "source_system = 'CARD_PLATFORM'"
    assert "leading zeros" in ev["pattern"]
    loss = next(f for f in report.symptoms if f.category == "ROW_LOSS")
    assert loss.expected - loss.actual == result.ground_truth["rows_affected"]
    assert loss.evidence["loan_types_missing_from_final"] == ["CREDIT_CARD"]


def test_duplicates_not_reported_twice(tmp_path):
    # final inherits staging's duplicates - that's a symptom, not a second root cause
    _, _, report = run_and_diagnose(tmp_path, Scenario.DUPLICATE_ROWS)
    dup_findings = [f for f in report.failures if f.category == "DUPLICATE_ROWS"]
    assert len(dup_findings) == 1
    assert "final.loan_portfolio" in dup_findings[0].evidence["persists_in"]


def test_report_saved(tmp_path):
    db, result, _ = run_and_diagnose(tmp_path, Scenario.NULL_SPIKE)
    with connect(db) as con:
        status, root = con.execute(
            "SELECT status, root_cause FROM meta.diagnostic_reports WHERE run_id = ?", [result.run_id]
        ).fetchone()
        snaps = con.execute("SELECT COUNT(*) FROM meta.layer_snapshots WHERE run_id = ?", [result.run_id]).fetchone()[0]
    assert (status, root) == ("FAILED", "NULL_SPIKE")
    assert snaps == 5


def test_llm_payload_hides_ground_truth(tmp_path):
    _, _, report = run_and_diagnose(tmp_path, Scenario.TYPE_CHANGE)
    assert "evaluation" not in report.for_llm()
