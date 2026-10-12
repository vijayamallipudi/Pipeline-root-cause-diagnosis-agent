"""Checks an `airflow dags test` run did what it should. Used by CI.

    python scripts/check_airflow_run.py <log file> <scenario>

Doesn't rely on `airflow dags test`'s exit code or log format - it checks what
the run left behind in DuckDB:
  - the latest run is for this scenario and finished all three stages
  - it was diagnosed, and the report task ran (so the gate ran *after* it)
  - healthy run -> gate passed; broken run -> gate raised DataQualityError
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb  # noqa: E402

from pipeline_rca import config  # noqa: E402


def _one(con, sql: str, params=()):
    """First row, or None - also when the table was never created because its task never ran."""
    try:
        return con.execute(sql, list(params)).fetchone()
    except duckdb.CatalogException:
        return None


def check(log: str, scenario: str, db_path=None) -> list[str]:
    problems = []
    con = duckdb.connect(str(db_path or config.DB_PATH), read_only=True)
    try:
        row = _one(con, "SELECT run_id, scenario, finished_at, row_counts FROM meta.pipeline_runs "
                        "ORDER BY started_at DESC LIMIT 1")
        if row is None:
            return ["no pipeline run was recorded"]
        run_id, ran, finished, counts = row
        if ran != scenario:
            problems.append(f"latest run is for {ran!r}, expected {scenario!r}")
        if finished is None:
            problems.append("run never finished (build_final didn't complete)")

        diag = _one(con, "SELECT status FROM meta.diagnostic_reports WHERE run_id = ?", [run_id])
        rca = _one(con, "SELECT source FROM meta.rca_reports WHERE run_id = ?", [run_id])
    finally:
        con.close()

    if diag is None:
        problems.append("run_diagnostics didn't save a report")
    if rca is None:
        problems.append("generate_rca_report didn't run")

    gate_failed = "DataQualityError" in log
    if scenario == "none":
        if diag and diag[0] != "HEALTHY":
            problems.append(f"healthy run diagnosed as {diag[0]}")
        if gate_failed:
            problems.append("quality gate failed on a healthy run")
    else:
        if diag and diag[0] != "FAILED":
            problems.append(f"broken run diagnosed as {diag[0]}")
        if not gate_failed:
            problems.append("quality gate didn't fail on a broken run")

    if not problems:
        print(f"ok: run {run_id} ({scenario}) -> {diag[0]}, report by {rca[0]}, "
              f"gate {'failed as expected' if gate_failed else 'passed'}")
    return problems


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    found = check(Path(sys.argv[1]).read_text(errors="replace"), sys.argv[2])
    for p in found:
        print(f"FAIL: {p}")
    sys.exit(1 if found else 0)
