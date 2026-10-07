#!/usr/bin/env bash
# Sets up Airflow for this project inside WSL / Linux / Mac.
#
#   bash scripts/airflow_setup.sh          # install (once)
#   bash scripts/airflow_setup.sh start    # run airflow standalone
#
# Airflow gets its own venv (~/.venvs/airflow-rca) and home (~/airflow-rca),
# and reads the DAG straight from this repo's dags/ folder.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="${AIRFLOW_VENV:-$HOME/.venvs/airflow-rca}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow-rca}"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export RCA_DB_PATH="${RCA_DB_PATH:-$REPO/data/pipeline.duckdb}"

pick_ollama_host() {
  # windows ollama is reachable on localhost with WSL mirrored networking;
  # otherwise try the windows host IP
  if curl -s -m 2 http://localhost:11434/api/tags >/dev/null; then
    echo "http://localhost:11434"
  else
    local host_ip
    host_ip="$(ip route show default 2>/dev/null | awk '{print $3}')"
    if [ -n "$host_ip" ] && curl -s -m 2 "http://$host_ip:11434/api/tags" >/dev/null; then
      echo "http://$host_ip:11434"
    else
      echo "http://localhost:11434"
    fi
  fi
}

if [ "${1:-}" = "start" ]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  export OLLAMA_HOST="${OLLAMA_HOST:-$(pick_ollama_host)}"
  if ! curl -s -m 2 "$OLLAMA_HOST/api/tags" >/dev/null; then
    echo "note: can't reach ollama at $OLLAMA_HOST - reports will use the template fallback"
  fi
  echo "dags:   $AIRFLOW__CORE__DAGS_FOLDER"
  echo "db:     $RCA_DB_PATH"
  echo "ollama: $OLLAMA_HOST"
  echo "UI:     http://localhost:8080  (login details are printed below by airflow)"
  exec airflow standalone
fi

echo "==> system packages"
if ! python3 -m venv --help >/dev/null 2>&1 || ! command -v curl >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3-venv python3-pip curl
fi

echo "==> venv at $VENV"
python3 -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install -q --upgrade pip

PY_VER="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-$(pip index versions apache-airflow 2>/dev/null | head -1 | sed -E 's/.*\((.*)\).*/\1/')}"
if [ -z "$AIRFLOW_VERSION" ]; then
  # `pip index` is experimental and sometimes prints nothing
  echo "couldn't look up the latest Airflow version - set it yourself, e.g.:"
  echo "  AIRFLOW_VERSION=3.1.0 bash scripts/airflow_setup.sh"
  exit 1
fi
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VER}.txt"
# Airflow only publishes constraints for the python versions it supports - check before installing
# so an unsupported python gives a clear message instead of a pip resolver error
if ! curl -sfI "$CONSTRAINTS" >/dev/null; then
  echo "Airflow $AIRFLOW_VERSION has no constraints file for python $PY_VER:"
  echo "  $CONSTRAINTS"
  echo "use a python version it supports, or pick another AIRFLOW_VERSION"
  exit 1
fi

echo "==> apache-airflow $AIRFLOW_VERSION (python $PY_VER)"
pip install -q "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINTS"

echo "==> this project"
pip install -q -e "$REPO"

echo "==> checking the DAG loads"
airflow dags list 2>/dev/null | grep pipeline_root_cause_diagnosis || {
  echo "DAG didn't load - import errors:"; airflow dags list-import-errors; exit 1;
}

echo
echo "Done. Start airflow with:  bash scripts/airflow_setup.sh start"
