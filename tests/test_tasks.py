"""Runs the Airflow task functions in order, the same way the DAG does, without Airflow.

Each call only gets the run_id (like XCom), so nothing is shared in memory
between steps - same as separate task processes.
"""

import json
from pathlib import Path

import pytest

from pipeline_rca import config, tasks
from pipeline_rca.llm import client
from pipeline_rca.pipeline.runner import get_run

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "pipeline_rca_dag.py"


@pytest.fixture(autouse=True)
def no_ollama(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(client, "is_available", lambda model=None: False)


def run_like_dag(db, scenario):
    run_id = tasks.start(scenario, db_path=db)
    tasks.extract(run_id, db_path=db)
    tasks.staging(run_id, db_path=db)
    tasks.final(run_id, db_path=db)
    summary = tasks.diagnostics(run_id, db_path=db)
    report = tasks.rca_report(run_id, db_path=db)
    json.dumps([summary, report])  # everything passed between tasks has to be XCom friendly
    return run_id, summary, report


def test_healthy_dag_run_passes_gate(tmp_path):
    db = tmp_path / "dag.duckdb"
    run_id, summary, report = run_like_dag(db, "none")
    assert summary["status"] == "HEALTHY"
    assert tasks.quality_gate(summary) == "HEALTHY"
    assert get_run(run_id, db_path=db).finished_at is not None


def test_failed_dag_run_writes_report_then_fails_gate(tmp_path):
    db = tmp_path / "dag.duckdb"
    run_id, summary, report = run_like_dag(db, "dropped_join_key")
    assert summary["root_cause"]["category"] == "JOIN_KEY_MISMATCH"
    assert Path(report["path"]).exists()
    with pytest.raises(tasks.DataQualityError, match="JOIN_KEY_MISMATCH"):
        tasks.quality_gate(summary)


def test_ground_truth_survives_separate_stages(tmp_path):
    # staging-stage chaos is recorded by the staging task, not the extract one
    db = tmp_path / "dag.duckdb"
    run_id, _, _ = run_like_dag(db, "duplicate_rows")
    run = get_run(run_id, db_path=db)
    assert run.ground_truth["injected_at"] == "staging:loans"
    assert set(run.row_counts) == {"raw", "staging", "final"}


def test_diagnose_uses_the_right_run(tmp_path):
    # two runs in the same db - the evaluation should use each run's own ground truth
    db = tmp_path / "dag.duckdb"
    first, _, _ = run_like_dag(db, "null_spike")
    second, summary, _ = run_like_dag(db, "none")
    assert summary["status"] == "HEALTHY"
    from pipeline_rca.diagnostics import load_report
    assert load_report(second, db_path=db).evaluation["scenario"] == "none"
    assert load_report(first, db_path=db).evaluation["scenario"] == "null_spike"


def test_dag_file_loads():
    pytest.importorskip("airflow", reason="airflow isn't installed (it doesn't run on Windows)")
    import importlib.util

    spec = importlib.util.spec_from_file_location("pipeline_rca_dag", DAG_FILE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    dag = mod.pipeline_root_cause_diagnosis()
    assert {t.task_id for t in dag.tasks} == {
        "start_run", "extract_to_raw", "transform_staging", "build_final",
        "run_diagnostics", "generate_rca_report", "quality_gate",
    }


# ---- DAG wiring, checked without airflow -------------------------------------
# airflow doesn't install on Windows, so load the real DAG file against a tiny
# stand-in for airflow.sdk that records tasks, dependencies and settings.

class _Node:
    def __init__(self, task_id, settings, args):
        self.task_id, self.settings = task_id, settings
        self.upstream = {a.task_id for a in args if isinstance(a, _Node)}

    def __rshift__(self, other):
        other.upstream.add(self.task_id)
        return other


def _fake_airflow():
    import types

    nodes = []

    def task(fn=None, **settings):
        def wrap(f):
            def make(*args, **kwargs):
                node = _Node(f.__name__, settings, args)
                nodes.append(node)
                return node
            return make
        return wrap(fn) if fn else wrap

    def dag(**dag_kwargs):
        def deco(fn):
            def build():
                nodes.clear()
                fn()
                return types.SimpleNamespace(kwargs=dag_kwargs, tasks={n.task_id: n for n in nodes})
            return build
        return deco

    class Param:
        def __init__(self, default, **kwargs):
            self.default, self.kwargs = default, kwargs

    sdk = types.ModuleType("airflow.sdk")
    sdk.dag, sdk.task, sdk.Param = dag, task, Param
    root = types.ModuleType("airflow")
    root.sdk = sdk
    return {"airflow": root, "airflow.sdk": sdk}


@pytest.fixture
def fake_dag(monkeypatch):
    import importlib.util
    import sys

    for name, mod in _fake_airflow().items():
        monkeypatch.setitem(sys.modules, name, mod)
    spec = importlib.util.spec_from_file_location("pipeline_rca_dag_fake", DAG_FILE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.pipeline_root_cause_diagnosis()


def test_dag_runs_tasks_in_order(fake_dag):
    order = ["start_run", "extract_to_raw", "transform_staging", "build_final",
             "run_diagnostics", "generate_rca_report", "quality_gate"]
    assert set(fake_dag.tasks) == set(order)
    for before, after in zip(order, order[1:]):
        assert before in fake_dag.tasks[after].upstream, f"{after} should wait for {before}"


def test_dag_retry_settings(fake_dag):
    assert fake_dag.kwargs["default_args"]["retries"] == 1
    assert fake_dag.tasks["quality_gate"].settings == {"retries": 0}
    timeout = fake_dag.tasks["generate_rca_report"].settings["execution_timeout"]
    # must outlast ollama's own timeout so the template fallback gets to run
    assert timeout.total_seconds() > config.OLLAMA_TIMEOUT


def test_dag_scenario_param_matches_chaos_scenarios(fake_dag):
    from pipeline_rca.chaos import Scenario

    param = fake_dag.kwargs["params"]["scenario"]
    assert param.default == "none"
    assert param.kwargs["enum"] == [s.value for s in Scenario]
    assert fake_dag.kwargs["max_active_runs"] == 1
