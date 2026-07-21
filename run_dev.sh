#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON_BIN:-python3}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8006}"

if [ ! -d ".venv" ]; then
  "$PYTHON_BIN" -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

export OPENAI_API_KEY="${OPENAI_API_KEY:-dummy}"
export OPENROUTER_API_KEY="${OPENROUTER_API_KEY:-dummy}"

exec .venv/bin/python -m uvicorn main:app --host "$HOST" --port "$PORT" --reload
