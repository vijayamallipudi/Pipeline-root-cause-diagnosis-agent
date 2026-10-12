"""The CI checker for `airflow dags test` runs - fed by the same task functions the DAG calls."""

import importlib.util
from pathlib import Path

import pytest

from pipeline_rca import config, tasks
from pipeline_rca.llm import client

spec = importlib.util.spec_from_file_location(
    "check_airflow_run", Path(__file__).resolve().parents[1] / "scripts" / "check_airflow_run.py"
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


@pytest.fixture(autouse=True)
def no_ollama(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(client, "is_available", lambda model=None: False)


def run_dag(db, scenario, stop_after=None):
    """Same order as the DAG; returns the 'log' the gate would produce."""
    run_id = tasks.start(scenario, db_path=db)
    steps = [
        lambda: tasks.extract(run_id, db_path=db),
        lambda: tasks.staging(run_id, db_path=db),
        lambda: tasks.final(run_id, db_path=db),
        lambda: tasks.diagnostics(run_id, db_path=db),
        lambda: tasks.rca_report(run_id, db_path=db),
    ]
    summary = None
    for i, step in enumerate(steps):
        if stop_after is not None and i > stop_after:
            return "task failed"
        out = step()
        summary = out if i == 3 else summary
    try:
        tasks.quality_gate(summary)
        return "quality_gate returned HEALTHY"
    except tasks.DataQualityError as e:
        return f"airflow.exceptions... DataQualityError: {e}"


def test_healthy_run_passes(tmp_path):
    db = tmp_path / "ci.duckdb"
    log = run_dag(db, "none")
    assert checker.check(log, "none", db_path=db) == []


def test_broken_run_passes_when_the_gate_fails(tmp_path):
    db = tmp_path / "ci.duckdb"
    log = run_dag(db, "dropped_join_key")
    assert "DataQualityError" in log
    assert checker.check(log, "dropped_join_key", db_path=db) == []


def test_catches_a_gate_that_didnt_fail(tmp_path):
    db = tmp_path / "ci.duckdb"
    run_dag(db, "dropped_join_key")
    assert "quality gate didn't fail on a broken run" in checker.check("all good", "dropped_join_key", db_path=db)


def test_catches_a_run_that_stopped_before_the_report(tmp_path):
    db = tmp_path / "ci.duckdb"
    log = run_dag(db, "none", stop_after=3)  # diagnostics ran, report didn't
    assert "generate_rca_report didn't run" in checker.check(log, "none", db_path=db)


def test_catches_the_wrong_scenario(tmp_path):
    db = tmp_path / "ci.duckdb"
    log = run_dag(db, "none")
    assert any("expected 'null_spike'" in p for p in checker.check(log, "null_spike", db_path=db))
