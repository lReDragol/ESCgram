#!/usr/bin/env bash
set -euo pipefail

APP_NAME="ESCgram"
PREFIX="${1:-$HOME/.local/opt/$APP_NAME}"
BIN_PATH="${2:-$HOME/.local/bin/escgram}"
DESKTOP_PATH="${XDG_DATA_HOME:-$HOME/.local/share}/applications/escgram.desktop"

rm -rf "$PREFIX"
rm -f "$BIN_PATH"
rm -f "$DESKTOP_PATH"

echo "ESCgram удалён из Linux-профиля."
