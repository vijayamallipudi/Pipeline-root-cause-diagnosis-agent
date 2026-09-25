"""Each check on its own: quiet on a clean run, and catches the chaos it's meant to catch."""

import pytest

from pipeline_rca.chaos import Scenario
from pipeline_rca.db import connect
from pipeline_rca.diagnostics import checks
from pipeline_rca.diagnostics.snapshot import take_snapshots
from pipeline_rca.pipeline import run_pipeline


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    """Run every scenario once and keep the db around for the checks."""
    out = {}
    for scenario in Scenario:
        db = tmp_path_factory.mktemp(scenario.value) / "checks.duckdb"
        run_pipeline(scenario, db_path=db)
        out[scenario] = db
    return out


def run_check(db, check):
    with connect(db) as con:
        return check(con, take_snapshots(con))


def failures(db, check):
    return [f for f in run_check(db, check) if f.failed]


@pytest.mark.parametrize("check", checks.ALL_CHECKS, ids=lambda c: c.__name__)
def test_clean_run_passes_every_check(runs, check):
    assert failures(runs[Scenario.NONE], check) == []


def test_withdrawn_drop_is_expected_not_a_failure(runs):
    info = run_check(runs[Scenario.NONE], checks.check_row_counts)
    assert len(info) == 1 and not info[0].failed
    assert "WITHDRAWN" in info[0].message


def test_schema_catches_type_change(runs):
    [f] = failures(runs[Scenario.TYPE_CHANGE], checks.check_schema)
    assert (f.table, f.column, f.actual) == ("raw.loans", "balance", "VARCHAR")
    assert f.evidence["affected_segment"] == "source_system = 'CORE_BANKING'"
    assert "currency" in f.evidence["pattern"]


def test_nulls_reported_where_they_start(runs):
    # type change: nulls first appear in staging (the cast), not in raw
    [f] = failures(runs[Scenario.TYPE_CHANGE], checks.check_nulls)
    assert (f.transition, f.table, f.column) == ("raw->staging", "staging.loans", "balance")
    assert f.evidence["upstream_dtype"] == "VARCHAR"

    # null spike: nulls arrive in raw and are only reported there
    [f] = failures(runs[Scenario.NULL_SPIKE], checks.check_nulls)
    assert (f.transition, f.table, f.column) == ("source->raw", "raw.customers", "credit_score")
    assert "final.loan_portfolio" in f.evidence["persists_in"]


def test_key_integrity_catches_stripped_zeros(runs):
    [f] = failures(runs[Scenario.DROPPED_JOIN_KEY], checks.check_key_integrity)
    assert f.evidence["affected_segment"] == "source_system = 'CARD_PLATFORM'"
    assert "leading zeros" in f.evidence["pattern"]


def test_row_loss_explained_by_unmatched_keys(runs):
    [f] = failures(runs[Scenario.DROPPED_JOIN_KEY], checks.check_row_counts)
    assert f.category == "ROW_LOSS" and f.transition == "staging->final"
    assert f.evidence["unmatched_rows"] == f.expected - f.actual
    assert f.evidence["match_if_zero_padded"] == f.evidence["unmatched_rows"]
    assert f.evidence["loan_types_missing_from_final"] == ["CREDIT_CARD"]


def test_duplicates_reported_once(runs):
    [f] = failures(runs[Scenario.DUPLICATE_ROWS], checks.check_uniqueness)
    assert f.table == "staging.loans"
    assert f.evidence["exact_copies"] == f.actual
    assert "final.loan_portfolio" in f.evidence["persists_in"]


def test_measures_find_the_segment(runs):
    [f] = failures(runs[Scenario.DROPPED_JOIN_KEY], checks.check_measures)
    worst = f.evidence["segments"][0]
    assert worst["source_system"] == "CARD_PLATFORM" and worst["actual"] == 0


def test_distribution_links_unknown_to_null_scores(runs):
    [f] = failures(runs[Scenario.NULL_SPIKE], checks.check_distribution)
    assert f.evidence["unknown_with_null_credit_score"] == f.evidence["unknown_rows"]
