"""
### Pipeline root-cause diagnosis

Runs the loan pipeline end to end, diagnoses it, writes a root-cause report
with a local LLM, then gates on data quality.

    start_run -> extract_to_raw -> transform_staging -> build_final
              -> run_diagnostics -> generate_rca_report -> quality_gate

Trigger it with a `scenario` param to inject a failure (`none` for a clean run).
The quality gate runs *after* the report so a failed run still leaves you a
write-up of what went wrong. The gate then fails the run so it shows red.

Retries:
- pipeline stages retry once - safe because re-running a stage for the same
  run is allowed (and another run's data is refused)
- the report task gets a hard time limit longer than the Ollama timeout, so a
  slow model falls back to the template report instead of being killed first
- the quality gate never retries - bad data doesn't fix itself, retrying
  would only delay the alert
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:  # airflow 3
    from airflow.sdk import Param, dag, task
except ImportError:  # airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param

from pipeline_rca import config, tasks
from pipeline_rca.chaos import Scenario

DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(seconds=30)}
# ollama gives up after OLLAMA_TIMEOUT and we fall back to the template - leave room for that
REPORT_TIMEOUT = timedelta(seconds=config.OLLAMA_TIMEOUT + 300)


@dag(
    dag_id="pipeline_root_cause_diagnosis",
    schedule=None,  # trigger manually with a scenario
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # duckdb allows one writer at a time
    default_args=DEFAULT_ARGS,
    tags=["data-quality", "root-cause", "duckdb", "ollama"],
    doc_md=__doc__,
    params={
        "scenario": Param(
            Scenario.NONE.value,
            enum=[s.value for s in Scenario],
            description="chaos scenario to inject (none = clean run)",
        ),
        "model": Param(config.OLLAMA_MODEL, type="string", description="ollama model for the report"),
    },
)
def pipeline_root_cause_diagnosis():
    @task
    def start_run(params=None) -> str:
        return tasks.start(params["scenario"])

    @task
    def extract_to_raw(run_id: str) -> dict:
        return tasks.extract(run_id)

    @task
    def transform_staging(run_id: str) -> dict:
        return tasks.staging(run_id)

    @task
    def build_final(run_id: str) -> dict:
        return tasks.final(run_id)

    @task
    def run_diagnostics(run_id: str) -> dict:
        return tasks.diagnostics(run_id)

    @task(execution_timeout=REPORT_TIMEOUT)
    def generate_rca_report(run_id: str, params=None) -> dict:
        return tasks.rca_report(run_id, model=params["model"])

    @task(retries=0)
    def quality_gate(summary: dict) -> str:
        return tasks.quality_gate(summary)

    run_id = start_run()
    raw = extract_to_raw(run_id)
    stg = transform_staging(run_id)
    fin = build_final(run_id)
    diag = run_diagnostics(run_id)
    report = generate_rca_report(run_id)
    gate = quality_gate(diag)

    raw >> stg >> fin >> diag >> report >> gate


pipeline_root_cause_diagnosis()
