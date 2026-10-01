"""Report generation. Ollama is faked here so the tests are fast and don't need a model.

Set RCA_TEST_OLLAMA=1 to also run one real call against your local ollama.
"""

import os

import pytest

from pipeline_rca import config
from pipeline_rca.chaos import Scenario
from pipeline_rca.db import connect
from pipeline_rca.diagnostics import diagnose
from pipeline_rca.llm import client, generate_report
from pipeline_rca.llm.report import REQUIRED_SECTIONS
from pipeline_rca.pipeline import run_pipeline


@pytest.fixture(scope="module")
def diagnosed(tmp_path_factory):
    out = {}
    for scenario in (Scenario.NONE, Scenario.DROPPED_JOIN_KEY):
        db = tmp_path_factory.mktemp(scenario.value) / "llm.duckdb"
        result = run_pipeline(scenario, db_path=db)
        out[scenario] = (db, diagnose(db_path=db, run_id=result.run_id))
    return out


@pytest.fixture(autouse=True)
def reports_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")


def fake_ollama(monkeypatch, text):
    calls = []
    monkeypatch.setattr(client, "is_available", lambda model=None: True)

    def chat(system, user, model=None, temperature=0.2, on_token=None):
        calls.append(user)
        return client.ChatResult(text=text, model=model or "fake", seconds=0.1)

    monkeypatch.setattr(client, "chat", chat)
    return calls


def has_all_sections(md):
    return all(f"## {h}" in md for h in REQUIRED_SECTIONS)


def test_healthy_run_skips_llm(diagnosed, monkeypatch):
    calls = fake_ollama(monkeypatch, "should not be used")
    db, diag = diagnosed[Scenario.NONE]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "template"
    assert calls == []
    assert "All checks passed" in rep.markdown


def test_falls_back_when_ollama_is_down(diagnosed, monkeypatch):
    monkeypatch.setattr(client, "is_available", lambda model=None: False)
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "template"
    assert "not available" in rep.note
    assert has_all_sections(rep.markdown)
    assert "staging.loans.customer_id" in rep.markdown


def test_uses_llm_text_and_fills_missing_sections(diagnosed, monkeypatch):
    llm_text = (
        "Sure, here's the report!\n\n## Summary\nThe customer_id key in staging.loans lost its leading zeros.\n\n"
        "## Where it broke\nraw->staging, loans.customer_id\n\n## Root cause\nKey cast to int."
    )
    fake_ollama(monkeypatch, llm_text)
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "llm"
    assert "Sure, here's" not in rep.markdown  # chatter before the first heading is dropped
    assert has_all_sections(rep.markdown)
    assert "Downstream impact" in rep.note and "Recommended fix" in rep.note
    assert "Evidence from automated checks" in rep.markdown


def test_rejects_ungrounded_llm_output(diagnosed, monkeypatch):
    fake_ollama(monkeypatch, "## Summary\nSomething went wrong with the database.")
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "template"
    assert rep.grounded is False



def test_report_saved(diagnosed, monkeypatch):
    monkeypatch.setattr(client, "is_available", lambda model=None: False)
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.path.exists()
    assert rep.path.read_text(encoding="utf-8") == rep.markdown
    with connect(db) as con:
        source = con.execute("SELECT source FROM meta.rca_reports WHERE run_id = ?", [diag.run_id]).fetchone()[0]
    assert source == "template"


@pytest.mark.skipif(os.getenv("RCA_TEST_OLLAMA") != "1", reason="set RCA_TEST_OLLAMA=1 to call a real model")
def test_real_ollama(diagnosed):
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "llm", rep.note
    assert has_all_sections(rep.markdown)


def evidence_block(markdown):
    return markdown.split("### Evidence from automated checks\n```\n", 1)[1].rsplit("\n```", 1)[0]


def test_code_fenced_llm_output_is_unwrapped(diagnosed, monkeypatch):
    # a leftover ``` would turn everything after it, including the evidence, into one code block
    fake_ollama(monkeypatch, "```markdown\n## Summary\nstaging.loans customer_id lost its leading zeros.\n```")
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    rep = generate_report(diag, db_path=db)
    assert rep.source == "llm"
    assert rep.markdown.count("```") == 2  # only the evidence block's own pair
    assert "```markdown" not in rep.markdown


def test_evidence_lines_fit_on_screen(diagnosed, monkeypatch):
    monkeypatch.setattr(client, "is_available", lambda model=None: False)
    db, diag = diagnosed[Scenario.DROPPED_JOIN_KEY]
    lines = evidence_block(generate_report(diag, db_path=db).markdown).splitlines()
    assert max(len(line) for line in lines) <= 100
    # one source system per line instead of one long joined line
    i = lines.index("  by source_system:")
    assert lines[i + 1].startswith("    CARD_PLATFORM:")
