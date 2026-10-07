"""Shell scripts: valid bash, and Unix line endings so they actually run on Linux/WSL."""

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = sorted((ROOT / "scripts").glob("*.sh"))


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_no_windows_line_endings(script):
    # a CRLF script fails on linux with "$'\r': command not found"
    assert b"\r\n" not in script.read_bytes()


def test_gitattributes_keeps_scripts_lf():
    # without this, git on windows can rewrite the scripts with CRLF on checkout
    assert "*.sh text eol=lf" in (ROOT / ".gitattributes").read_text()


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")
@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_bash_syntax(script):
    out = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


def test_setup_script_checks_before_installing():
    s = (ROOT / "scripts" / "airflow_setup.sh").read_text()
    # fail clearly instead of with a pip resolver error
    assert 'if [ -z "$AIRFLOW_VERSION" ]' in s
    assert s.index('curl -sfI "$CONSTRAINTS"') < s.index('pip install -q "apache-airflow')
