"""Running stages one at a time, the way separate Airflow tasks will."""

import pytest

from pipeline_rca.pipeline.runner import (
    StageOrderError,
    UnknownRunError,
    finish_run,
    get_run,
    run_pipeline,
    run_stage,
    start_run,
)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "runner.duckdb"


def run_all(db, scenario):
    run_id = start_run(scenario, db_path=db)
    for stage in ("extract", "staging", "final"):
        run_stage(stage, run_id, db_path=db)
    return finish_run(run_id, db_path=db)


def test_stage_by_stage_matches_one_shot(db, tmp_path):
    by_stage = run_all(db, "dropped_join_key")
    one_shot = run_pipeline("dropped_join_key", db_path=tmp_path / "other.duckdb")
    assert by_stage.row_counts == one_shot.row_counts
    assert by_stage.ground_truth == one_shot.ground_truth
    assert by_stage.finished_at is not None


def test_ground_truth_recorded_by_the_stage_that_injects(db):
    # duplicate_rows fires during staging - a separate injector from extract's
    result = run_all(db, "duplicate_rows")
    assert result.ground_truth["injected_at"] == "staging:loans"


def test_unknown_run_on_a_fresh_db(db):
    with pytest.raises(UnknownRunError, match="start_run"):
        run_stage("extract", "nope", db_path=db)
    with pytest.raises(UnknownRunError):
        finish_run("nope", db_path=db)


def test_unknown_run_on_a_used_db(db):
    run_all(db, "none")
    with pytest.raises(UnknownRunError):
        get_run("nope", db_path=db)


def test_stage_before_its_input_is_refused(db):
    run_id = start_run("none", db_path=db)
    with pytest.raises(StageOrderError, match="Run 'extract'"):
        run_stage("staging", run_id, db_path=db)
    with pytest.raises(StageOrderError, match="Run 'staging'"):
        run_stage("final", run_id, db_path=db)


def test_run_cant_read_another_runs_layer(db):
    # B skipping extract used to silently clean A's raw data
    a = start_run("duplicate_rows", db_path=db)
    run_stage("extract", a, db_path=db)
    b = start_run("type_change", db_path=db)
    with pytest.raises(StageOrderError, match=f"data from run {a}"):
        run_stage("staging", b, db_path=db)


def test_interleaved_runs_are_caught(db):
    a = start_run("none", db_path=db)
    b = start_run("none", db_path=db)
    run_stage("extract", a, db_path=db)
    run_stage("extract", b, db_path=db)  # b overwrites raw
    with pytest.raises(StageOrderError):
        run_stage("staging", a, db_path=db)
    run_stage("staging", b, db_path=db)  # b can carry on


def test_rerunning_a_stage_is_fine(db):
    run_id = start_run("none", db_path=db)
    run_stage("extract", run_id, db_path=db)
    run_stage("extract", run_id, db_path=db)  # e.g. an airflow retry
    run_stage("staging", run_id, db_path=db)


def test_bad_stage_name(db):
    run_id = start_run("none", db_path=db)
    with pytest.raises(ValueError, match="unknown stage"):
        run_stage("transform", run_id, db_path=db)
