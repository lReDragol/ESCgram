#!/usr/bin/env bash
set -euo pipefail

APP_NAME="ESCgram"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_SOURCE_DIR="$SCRIPT_DIR/app"

PREFIX="${1:-$HOME/.local/opt/$APP_NAME}"
BIN_DIR="${2:-$HOME/.local/bin}"
DATA_DIR_DEFAULT="${XDG_DATA_HOME:-$HOME/.local/share}/$APP_NAME"
DESKTOP_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/applications"

if [[ ! -d "$APP_SOURCE_DIR" ]]; then
  echo "Папка app/ не найдена рядом с install.sh"
  exit 1
fi

mkdir -p "$PREFIX" "$BIN_DIR" "$DESKTOP_DIR"

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete "$APP_SOURCE_DIR"/ "$PREFIX"/
else
  rm -rf "$PREFIX"/*
  cp -a "$APP_SOURCE_DIR"/. "$PREFIX"/
fi

cat >"$BIN_DIR/escgram" <<EOF
#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$PREFIX"
DATA_DIR="\${ESCGRAM_DATA_DIR:-$DATA_DIR_DEFAULT}"
mkdir -p "\$DATA_DIR"
exec "\$APP_DIR/ESCgram" --data-dir "\$DATA_DIR" "\$@"
EOF
chmod +x "$BIN_DIR/escgram"

cat >"$DESKTOP_DIR/escgram.desktop" <<EOF
[Desktop Entry]
Name=ESCgram
Comment=Telegram client
Exec=$BIN_DIR/escgram
Terminal=false
Type=Application
Categories=Network;InstantMessaging;
StartupNotify=true
EOF

echo
echo "ESCgram установлен."
echo "Бинарник: $BIN_DIR/escgram"
echo "Данные: $DATA_DIR_DEFAULT"
