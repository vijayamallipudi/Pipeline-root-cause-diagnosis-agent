"""Clicks through the Streamlit app headlessly. Ollama is switched off so it's quick."""

from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from pipeline_rca import config  # noqa: E402
from pipeline_rca.db import connect  # noqa: E402
from pipeline_rca.llm import client  # noqa: E402

APP = str(Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py")


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "app.duckdb")
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(client, "available_models", lambda: [])
    monkeypatch.setattr(client, "is_available", lambda model=None: False)
    return AppTest.from_file(APP, default_timeout=120).run()


def run_scenario(at, scenario):
    at.sidebar.selectbox[0].set_value(scenario)
    at.sidebar.button[0].click().run()
    assert not at.exception, at.exception
    return at


def test_empty_state(app):
    assert not app.exception
    assert "Run pipeline" in app.info[0].value


def test_failed_run_shows_root_cause(app):
    at = run_scenario(app, "dropped_join_key")
    assert "JOIN_KEY_MISMATCH" in at.error[0].value
    assert [m.value for m in at.metric] == ["5,000", "4,859", "3,378"]
    assert "Diagnosis matches what was injected." in [s.value for s in at.success]


def test_report_written_on_demand(app):
    at = run_scenario(app, "type_change")
    [write] = [b for b in at.button if b.label.startswith("Write the report")]
    write.click().run()
    assert not at.exception
    assert any("## Recommended fix" in m.value for m in at.markdown)


def test_healthy_run(app):
    at = run_scenario(app, "none")
    assert any("HEALTHY" in s.value for s in at.success)


def test_history_follows_new_runs(app):
    at = run_scenario(app, "null_spike")
    at = run_scenario(at, "duplicate_rows")
    radio = at.sidebar.radio[0]
    assert len(radio.options) == 2
    assert radio.index == 0  # newest run is selected
    assert "Duplicate rows" in radio.options[0]
    assert "DUPLICATE_ROWS" in at.error[0].value

    with connect() as con:
        older = con.execute("SELECT run_id FROM meta.pipeline_runs ORDER BY started_at LIMIT 1").fetchone()[0]
    radio.set_value(older).run()
    assert "NULL_SPIKE" in at.error[0].value
    assert any("most recent run" in i.value for i in at.info)  # data tab explains it can't show old tables


def test_picking_a_scenario_without_running_says_so(app):
    at = run_scenario(app, "none")
    assert at.subheader[0].value == "Showing: Healthy run"
    at.sidebar.selectbox[0].set_value("null_spike").run()
    assert any("haven't run it yet" in i.value for i in at.info)
    at.sidebar.button[0].click().run()
    assert at.subheader[0].value == "Showing: Null spike"
    assert not any("haven't run it yet" in i.value for i in at.info)


def test_guide_open_for_first_visit_then_collapsed(app):
    assert app.expander[0].label.startswith("👋 New here?")
    assert app.expander[0].proto.expanded
    at = run_scenario(app, "type_change")
    assert not at.expander[0].proto.expanded
    assert "Problem detected in the data" in at.error[0].value
    assert "caught the problem you injected" in at.error[0].value


def test_busy_database_shows_a_message_not_a_crash(app, tmp_path):
    import subprocess
    import sys
    import time

    # another process (like the CLI or an airflow task) holding the db open
    holder = subprocess.Popen(
        [sys.executable, "-c",
         f"import duckdb, time; con = duckdb.connect(r'{tmp_path / 'app.duckdb'}'); print('ready', flush=True); time.sleep(60)"],
        stdout=subprocess.PIPE, text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        time.sleep(0.5)
        at = app.run()
        assert not at.exception, at.exception
        assert any("database is busy" in w.value for w in at.warning)
    finally:
        holder.kill()
        holder.wait()

    at = at.run()  # once it's free the page works again
    assert not any("database is busy" in w.value for w in at.warning)
