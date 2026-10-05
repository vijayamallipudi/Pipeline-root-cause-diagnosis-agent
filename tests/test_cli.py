"""Runs scripts/run_pipeline.py the way a person (or cron/CI) would - as a separate process."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_pipeline.py"


@pytest.fixture
def cli(tmp_path):
    env = {
        **os.environ,
        "RCA_REPORTS_DIR": str(tmp_path / "reports"),
        "OLLAMA_HOST": "http://127.0.0.1:9",  # nothing listens here - forces the template report
        "PYTHONIOENCODING": "utf-8",
    }

    def run(*args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--db", str(tmp_path / "cli.duckdb"), *args],
            capture_output=True, text=True, encoding="utf-8", env=env, timeout=300,
        )

    return run


def test_list_shows_every_scenario(cli):
    out = cli("--list")
    assert out.returncode == 0
    for name in ("none", "type_change", "null_spike", "dropped_join_key", "duplicate_rows"):
        assert name in out.stdout


def test_healthy_run(cli):
    out = cli()
    assert out.returncode == 0, out.stderr
    assert "Diagnosis: HEALTHY" in out.stdout
    assert "WITHDRAWN" in out.stdout


def test_failed_run_prints_root_cause(cli):
    out = cli("--scenario", "dropped_join_key")
    assert out.returncode == 0, out.stderr
    assert "Root cause  [JOIN_KEY_MISMATCH] at raw->staging" in out.stdout
    assert "leading zeros stripped" in out.stdout
    assert "Check vs injected chaos: correct" in out.stdout


def test_json_is_valid(cli):
    out = cli("--scenario", "null_spike", "--json")
    report = json.loads(out.stdout)
    assert report["status"] == "FAILED"
    assert report["root_cause"]["category"] == "NULL_SPIKE"
    assert "rca_report" not in report


def test_explain_falls_back_to_template_without_ollama(cli, tmp_path):
    out = cli("--scenario", "type_change", "--explain")
    assert out.returncode == 0, out.stderr
    assert "[template]" in out.stdout
    assert "## Recommended fix" in out.stdout
    assert list((tmp_path / "reports").glob("*.md"))


def test_json_with_explain_includes_the_report(cli):
    # used to silently drop the report - the json branch returned before --explain ran
    report = json.loads(cli("--scenario", "duplicate_rows", "--json", "--explain").stdout)
    assert report["rca_report"]["source"] == "template"
    assert "## Root cause" in report["rca_report"]["markdown"]


def test_fail_on_problem_exit_codes(cli):
    assert cli("--scenario", "duplicate_rows", "--fail-on-problem").returncode == 1
    assert cli("--fail-on-problem").returncode == 0
    assert cli("--scenario", "duplicate_rows").returncode == 0  # off by default


def test_unknown_scenario_is_rejected(cli):
    out = cli("--scenario", "meteor_strike")
    assert out.returncode == 2
    assert "invalid choice" in out.stderr
