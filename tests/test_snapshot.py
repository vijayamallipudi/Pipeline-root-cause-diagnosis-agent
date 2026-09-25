"""Snapshots should capture what each layer actually looks like."""

import json

from pipeline_rca.chaos import Scenario
from pipeline_rca.db import connect
from pipeline_rca.diagnostics.contracts import CONTRACTS
from pipeline_rca.diagnostics.snapshot import save_snapshots, take_snapshots
from pipeline_rca.pipeline import run_pipeline


def snapshots_for(tmp_path, scenario):
    db = tmp_path / "snap.duckdb"
    result = run_pipeline(scenario, db_path=db)
    with connect(db) as con:
        return db, result, take_snapshots(con)


def test_every_contract_table_is_profiled(tmp_path):
    _, result, snaps = snapshots_for(tmp_path, Scenario.NONE)
    assert set(snaps) == {c.name for c in CONTRACTS}
    assert snaps["raw.loans"].row_count == result.row_counts["raw"]["loans"]
    assert snaps["final.loan_portfolio"].row_count == result.row_counts["final"]["loan_portfolio"]


def test_clean_run_matches_contracts(tmp_path):
    _, _, snaps = snapshots_for(tmp_path, Scenario.NONE)
    for c in CONTRACTS:
        snap = snaps[c.name]
        assert snap.duplicate_keys == 0
        for col, expected_type in c.columns.items():
            assert snap.columns[col].dtype == expected_type, f"{c.name}.{col}"
            assert snap.columns[col].null_pct <= c.null_limit(col)


def test_profile_picks_up_chaos(tmp_path):
    _, _, snaps = snapshots_for(tmp_path, Scenario.TYPE_CHANGE)
    assert snaps["raw.loans"].columns["balance"].dtype == "VARCHAR"
    assert snaps["staging.loans"].columns["balance"].null_pct > 50


def test_numeric_stats(tmp_path):
    _, _, snaps = snapshots_for(tmp_path, Scenario.NONE)
    score = snaps["staging.customers"].columns["credit_score"]
    assert 300 <= score.min <= score.max <= 850
    assert snaps["staging.loans"].columns["balance"].sum > 0


def test_snapshots_saved(tmp_path):
    db, result, snaps = snapshots_for(tmp_path, Scenario.NONE)
    with connect(db) as con:
        save_snapshots(con, result.run_id, snaps)
        rows = con.execute("SELECT table_name, profile FROM meta.layer_snapshots WHERE run_id = ?",
                           [result.run_id]).fetchall()
    assert len(rows) == len(CONTRACTS)
    assert "columns" in json.loads(rows[0][1])
