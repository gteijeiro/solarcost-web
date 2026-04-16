#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"

if [ ! -x "$VENV_DIR/bin/sa_web" ]; then
  echo "No se encontro $VENV_DIR/bin/sa_web"
  exit 1
fi

exec "$VENV_DIR/bin/sa_web" uninstall "$@"
