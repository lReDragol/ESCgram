#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Виртуальное окружение не найдено. Сначала запустите scripts/install_linux.sh"
  exit 1
fi

source .venv/bin/activate
exec python main.py "$@"
