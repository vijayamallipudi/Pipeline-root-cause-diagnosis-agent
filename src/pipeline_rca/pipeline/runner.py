"""Runs raw -> staging -> final and logs the run to meta.pipeline_runs."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from pipeline_rca import config
from pipeline_rca.chaos import ChaosInjector, Scenario
from pipeline_rca.db import connect
from pipeline_rca.pipeline.extract import extract
from pipeline_rca.pipeline.final import build_final
from pipeline_rca.pipeline.staging import transform_staging

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
    finished_at: datetime
    row_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    ground_truth: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["started_at"] = self.started_at.isoformat()
        d["finished_at"] = self.finished_at.isoformat()
        return d


def _record_run(result: PipelineResult, db_path) -> None:
    with connect(db_path) as con:
        con.execute(RUNS_DDL)
        con.execute(
            f"INSERT INTO {config.META}.pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                result.run_id,
                result.scenario,
                result.seed,
                result.started_at,
                result.finished_at,
                json.dumps(result.row_counts),
                json.dumps(result.ground_truth),
            ],
        )


def run_pipeline(
    scenario: Scenario | str = Scenario.NONE,
    db_path=None,
    seed: int = config.DEFAULT_SEED,
) -> PipelineResult:
    injector = ChaosInjector(scenario=Scenario(scenario), seed=seed)
    started = datetime.now(timezone.utc).replace(tzinfo=None)

    row_counts = {
        config.RAW: extract(injector, db_path=db_path, seed=seed),
        config.STAGING: transform_staging(injector, db_path=db_path),
        config.FINAL: build_final(db_path=db_path),
    }

    result = PipelineResult(
        run_id=uuid.uuid4().hex[:12],
        scenario=injector.scenario.value,
        seed=seed,
        started_at=started,
        finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
        row_counts=row_counts,
        ground_truth=injector.ground_truth,
    )
    _record_run(result, db_path)
    return result
