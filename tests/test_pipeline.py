"""Clean run should pass, and each scenario should break what it says it breaks."""

import pytest

from pipeline_rca.chaos import ChaosInjector, Scenario
from pipeline_rca.data_generator import generate_source_data
from pipeline_rca.db import connect
from pipeline_rca.pipeline import run_pipeline


@pytest.fixture
def db(tmp_path):
    return tmp_path / "test.duckdb"


def q(db, sql):
    with connect(db) as con:
        return con.execute(sql).fetchone()[0]


def test_generator_is_deterministic():
    a, b = generate_source_data(seed=7), generate_source_data(seed=7)
    assert a["loans"].equals(b["loans"])
    assert a["customers"].equals(b["customers"])


def test_injector_ignores_other_hooks():
    loans = generate_source_data()["loans"]
    injector = ChaosInjector(Scenario.DUPLICATE_ROWS)
    assert injector.apply("extract", "loans", loans) is loans
    assert injector.ground_truth == {}


def test_healthy_run(db):
    result = run_pipeline(Scenario.NONE, db_path=db)
    staged = result.row_counts["staging"]["loans"]
    # only the WITHDRAWN filter should drop loans, the join shouldn't lose any
    assert staged < result.row_counts["raw"]["loans"]
    assert result.row_counts["final"]["loan_portfolio"] == staged
    assert result.ground_truth == {}
    assert q(db, "SELECT COUNT(*) FROM final.loan_portfolio WHERE balance IS NULL") == 0
    assert q(db, "SELECT COUNT(*) FROM final.loan_portfolio WHERE risk_band = 'UNKNOWN'") == 0
    assert q(db, "SELECT COUNT(*) FROM meta.pipeline_runs") == 1


def test_type_change(db):
    result = run_pipeline(Scenario.TYPE_CHANGE, db_path=db)
    assert q(db, "SELECT typeof(balance) FROM raw.loans LIMIT 1") == "VARCHAR"
    nulls = q(db, "SELECT COUNT(*) FROM staging.loans WHERE balance IS NULL")
    assert nulls > 0
    assert result.ground_truth["column"] == "balance"


def test_null_spike(db):
    run_pipeline(Scenario.NULL_SPIKE, db_path=db)
    null_pct = q(db, "SELECT AVG((credit_score IS NULL)::INT) FROM staging.customers")
    assert 0.25 < null_pct < 0.45
    assert q(db, "SELECT COUNT(*) FROM final.loan_portfolio WHERE risk_band = 'UNKNOWN'") > 0


def test_dropped_join_key(db):
    result = run_pipeline(Scenario.DROPPED_JOIN_KEY, db_path=db)
    lost = result.row_counts["staging"]["loans"] - result.row_counts["final"]["loan_portfolio"]
    assert lost == result.ground_truth["rows_affected"]
    assert q(db, "SELECT COUNT(*) FROM final.loan_portfolio WHERE source_system = 'CARD_PLATFORM'") == 0


def test_duplicate_rows(db):
    result = run_pipeline(Scenario.DUPLICATE_ROWS, db_path=db)
    dupes = q(db, "SELECT COUNT(*) - COUNT(DISTINCT loan_id) FROM final.loan_portfolio")
    assert dupes == result.ground_truth["rows_affected"] > 0
