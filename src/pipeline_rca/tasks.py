"""One function per Airflow task.

Kept separate from the DAG file so the logic can be run and tested without
Airflow installed. Every function takes/returns small JSON-friendly values
(run_id, dicts) because that's what gets passed between tasks through XCom.
"""

from __future__ import annotations

from pipeline_rca import config
from pipeline_rca.chaos import Scenario
from pipeline_rca.diagnostics import diagnose, load_report
from pipeline_rca.llm import generate_report
from pipeline_rca.pipeline.runner import finish_run, run_stage, start_run


class DataQualityError(RuntimeError):
    """Raised by the quality gate so the DAG run shows up as failed."""


def start(scenario: str = Scenario.NONE.value, seed: int = config.DEFAULT_SEED, db_path=None) -> str:
    return start_run(scenario, db_path=db_path, seed=seed)


def extract(run_id: str, db_path=None) -> dict:
    return run_stage("extract", run_id, db_path=db_path)


def staging(run_id: str, db_path=None) -> dict:
    return run_stage("staging", run_id, db_path=db_path)


def final(run_id: str, db_path=None) -> dict:
    counts = run_stage("final", run_id, db_path=db_path)
    finish_run(run_id, db_path=db_path)
    return counts


def diagnostics(run_id: str, db_path=None) -> dict:
    report = diagnose(db_path=db_path, run_id=run_id)
    rc = report.root_cause
    return {
        "run_id": run_id,
        "status": report.status,
        "checks": report.checks,
        "root_cause": None if rc is None else {
            "category": rc.category,
            "transition": rc.transition,
            "table": rc.table,
            "column": rc.column,
            "message": rc.message,
        },
        "symptoms": len(report.symptoms),
    }


def rca_report(run_id: str, model: str | None = None, db_path=None) -> dict:
    report = generate_report(load_report(run_id, db_path=db_path), db_path=db_path, model=model)
    return {
        "run_id": run_id,
        "source": report.source,
        "model": report.model,
        "seconds": report.seconds,
        "note": report.note,
        "path": str(report.path) if report.path else None,
    }


def quality_gate(summary: dict) -> str:
    """Fail loudly if diagnostics found a problem. The report has already been written by now."""
    if summary["status"] == "HEALTHY":
        return "HEALTHY"
    rc = summary["root_cause"]
    raise DataQualityError(
        f"run {summary['run_id']} failed data quality checks - root cause: "
        f"{rc['category']} at {rc['transition']} ({rc['table']}.{rc['column']}): {rc['message']}"
    )
