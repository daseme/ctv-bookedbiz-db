#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source .venv/bin/activate

set -a
source .env.dev
set +a

mkdir -p "${DATA_PATH:?}"

exec uvicorn src.web.asgi:app --host 0.0.0.0 --port "${PORT:-5100}"
