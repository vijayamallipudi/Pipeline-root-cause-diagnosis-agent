"""Runs raw -> staging -> final and logs the run to meta.pipeline_runs.

Each stage can run on its own (that's how Airflow runs them - separate task,
separate process), so all run state lives in meta.pipeline_runs rather than
in memory:

    run_id = start_run("null_spike")
    run_stage("extract", run_id)
    run_stage("staging", run_id)
    run_stage("final", run_id)
    result = finish_run(run_id)

run_pipeline() does all of that in one go.

Each layer also remembers which run last wrote it (meta.layer_owner). A stage
refuses to read a layer some other run wrote - otherwise staging for run B,
started before B's extract, would quietly clean run A's raw data.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import duckdb

from pipeline_rca import config
from pipeline_rca.chaos import ChaosInjector, Scenario
from pipeline_rca.db import connect
from pipeline_rca.pipeline.extract import extract
from pipeline_rca.pipeline.final import build_final
from pipeline_rca.pipeline.staging import transform_staging

STAGES = ("extract", "staging", "final")
STAGE_LAYER = {"extract": config.RAW, "staging": config.STAGING, "final": config.FINAL}
# the stage that has to run first, for the same run
NEEDS = {"staging": "extract", "final": "staging"}


class UnknownRunError(KeyError):
    pass


class StageOrderError(RuntimeError):
    pass


OWNER_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.META}.layer_owner (
    layer       VARCHAR PRIMARY KEY,
    run_id      VARCHAR,
    written_at  TIMESTAMP
)
"""

RUNS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.META}.pipeline_runs (
    run_id        VARCHAR PRIMARY KEY,
    scenario      VARCHAR,
    seed          INTEGER,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    row_counts    JSON,
    ground_truth  JSON
)
"""


@dataclass
class PipelineResult:
    run_id: str
    scenario: str
    seed: int
    started_at: datetime
    finished_at: datetime | None
    row_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    ground_truth: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["started_at"] = self.started_at.isoformat()
        d["finished_at"] = self.finished_at.isoformat() if self.finished_at else None
        return d


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _load(con, run_id: str) -> PipelineResult:
    try:
        row = con.execute(
            f"SELECT run_id, scenario, seed, started_at, finished_at, row_counts, ground_truth "
            f"FROM {config.META}.pipeline_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
    except duckdb.CatalogException:  # brand new db - no runs table yet
        row = None
    if row is None:
        raise UnknownRunError(f"no pipeline run with id {run_id!r} - create one with start_run() first")
    rid, scenario, seed, started, finished, counts, truth = row
    return PipelineResult(rid, scenario, seed, started, finished,
                          json.loads(counts) if counts else {}, json.loads(truth) if truth else {})


def start_run(
    scenario: Scenario | str = Scenario.NONE,
    db_path=None,
    seed: int = config.DEFAULT_SEED,
    run_id: str | None = None,
) -> str:
    run_id = run_id or uuid.uuid4().hex[:12]
    with connect(db_path) as con:
        con.execute(RUNS_DDL)
        con.execute(
            f"INSERT INTO {config.META}.pipeline_runs VALUES (?, ?, ?, ?, NULL, '{{}}', '{{}}')",
            [run_id, Scenario(scenario).value, seed, _now()],
        )
    return run_id


def run_stage(stage: str, run_id: str, db_path=None) -> dict[str, int]:
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}, expected one of {STAGES}")
    with connect(db_path) as con:
        run = _load(con, run_id)
        con.execute(OWNER_DDL)
        if stage in NEEDS:
            source = STAGE_LAYER[NEEDS[stage]]
            owner = con.execute(f"SELECT run_id FROM {config.META}.layer_owner WHERE layer = ?",
                                [source]).fetchone()
            if owner is None or owner[0] != run_id:
                written_by = f"run {owner[0]}" if owner else "no run yet"
                raise StageOrderError(
                    f"can't run '{stage}' for run {run_id}: the {source} layer holds data from {written_by}. "
                    f"Run '{NEEDS[stage]}' for {run_id} first."
                )

    # a fresh injector per stage - it only fires at its own hook point
    injector = ChaosInjector(scenario=Scenario(run.scenario), seed=run.seed)
    if stage == "extract":
        counts = extract(injector, db_path=db_path, seed=run.seed)
    elif stage == "staging":
        counts = transform_staging(injector, db_path=db_path)
    else:
        counts = build_final(db_path=db_path)

    with connect(db_path) as con:
        run = _load(con, run_id)
        run.row_counts[STAGE_LAYER[stage]] = counts
        if injector.ground_truth:
            run.ground_truth = injector.ground_truth
        con.execute(
            f"UPDATE {config.META}.pipeline_runs SET row_counts = ?, ground_truth = ? WHERE run_id = ?",
            [json.dumps(run.row_counts), json.dumps(run.ground_truth), run_id],
        )
        con.execute(f"INSERT OR REPLACE INTO {config.META}.layer_owner VALUES (?, ?, ?)",
                    [STAGE_LAYER[stage], run_id, _now()])
    return counts


def finish_run(run_id: str, db_path=None) -> PipelineResult:
    with connect(db_path) as con:
        _load(con, run_id)  # clear error for an unknown id
        con.execute(f"UPDATE {config.META}.pipeline_runs SET finished_at = ? WHERE run_id = ?", [_now(), run_id])
        return _load(con, run_id)


def get_run(run_id: str, db_path=None) -> PipelineResult:
    with connect(db_path) as con:
        return _load(con, run_id)


def run_pipeline(
    scenario: Scenario | str = Scenario.NONE,
    db_path=None,
    seed: int = config.DEFAULT_SEED,
) -> PipelineResult:
    run_id = start_run(scenario, db_path=db_path, seed=seed)
    for stage in STAGES:
        run_stage(stage, run_id, db_path=db_path)
    return finish_run(run_id, db_path=db_path)
