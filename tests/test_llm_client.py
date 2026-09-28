"""Ollama client and prompt building - no real model needed."""

import json
from contextlib import contextmanager

import pytest

from pipeline_rca import config
from pipeline_rca.chaos import Scenario
from pipeline_rca.diagnostics import diagnose
from pipeline_rca.llm import client
from pipeline_rca.llm.prompts import FIX_PLAYBOOK, SYSTEM_PROMPT, build_facts
from pipeline_rca.pipeline import run_pipeline


@pytest.fixture(scope="module")
def diag(tmp_path_factory):
    db = tmp_path_factory.mktemp("prompt") / "p.duckdb"
    result = run_pipeline(Scenario.DROPPED_JOIN_KEY, db_path=db)
    return diagnose(db_path=db, run_id=result.run_id)


def facts_for(d):
    return build_facts(d.for_llm(), d.root_cause, d.symptoms, d.expected_changes)


def test_facts_carry_the_evidence(diag):
    facts = facts_for(diag)
    assert "staging.loans.customer_id" in facts
    assert "CARD_PLATFORM" in facts
    assert "leading zeros stripped" in facts
    assert "1,481" in facts


def test_facts_separate_expected_changes(diag):
    facts = facts_for(diag)
    expected_block = facts.split("EXPECTED CHANGES")[1].split("FINAL LAYER")[0]
    assert "WITHDRAWN" in expected_block


def test_facts_never_leak_the_answer(diag):
    assert "dropped_join_key" not in facts_for(diag)
    assert "evaluation" not in diag.for_llm()


def test_fix_playbook_matches_root_cause(diag):
    facts = facts_for(diag)
    for step in FIX_PLAYBOOK["JOIN_KEY_MISMATCH"]:
        assert step in facts


def test_system_prompt_asks_for_all_sections():
    for heading in ("## Summary", "## Where it broke", "## Root cause", "## Downstream impact", "## Recommended fix"):
        assert heading in SYSTEM_PROMPT


def test_model_name_matching(monkeypatch):
    monkeypatch.setattr(client, "available_models", lambda: ["llama3.2:latest", "llama3.2:1b"])
    assert client.is_available("llama3.2")
    assert client.is_available("llama3.2:1b")
    assert not client.is_available("mistral")


def test_unreachable_ollama_is_handled(monkeypatch):
    monkeypatch.setattr(config, "OLLAMA_HOST", "http://127.0.0.1:9")
    assert client.available_models() == []
    with pytest.raises(client.OllamaError):
        client.chat("sys", "user", model="x")


def test_chat_without_streaming(monkeypatch):
    monkeypatch.setattr(client, "_request", lambda path, payload, timeout: {"message": {"content": " hello "}})
    assert client.chat("sys", "user", model="m").text == "hello"


def test_chat_streams_tokens(monkeypatch):
    lines = [json.dumps({"message": {"content": c}, "done": False}).encode() for c in ("Hel", "lo ", "there")]
    lines.append(json.dumps({"message": {"content": ""}, "done": True}).encode())

    @contextmanager
    def fake_open(path, payload, timeout):
        assert payload["stream"] is True
        yield iter(lines)

    monkeypatch.setattr(client, "_open", fake_open)
    seen = []
    result = client.chat("sys", "user", model="m", on_token=seen.append)
    assert seen == ["Hel", "lo ", "there"]
    assert result.text == "Hello there"


def test_empty_response_is_an_error(monkeypatch):
    monkeypatch.setattr(client, "_request", lambda path, payload, timeout: {"message": {"content": "  "}})
    with pytest.raises(client.OllamaError, match="empty"):
        client.chat("sys", "user", model="m")
