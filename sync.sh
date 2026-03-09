#!/usr/bin/env bash
set -euo pipefail

PLUGIN_ID="hackernews"
EXECUTABLE="tabularis-hackernews-plugin"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$(uname -s)" in
  Linux*)  PLUGINS_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/tabularis/plugins" ;;
  Darwin*) PLUGINS_DIR="$HOME/Library/Application Support/com.debba.tabularis/plugins" ;;
  CYGWIN*|MINGW*|MSYS*) PLUGINS_DIR="${APPDATA}/com.debba.tabularis/plugins" ;;
  *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

echo "Building (cargo build --release)…"
cargo build --release --manifest-path "$SCRIPT_DIR/Cargo.toml"

DEST="$PLUGINS_DIR/$PLUGIN_ID"
mkdir -p "$DEST"

cp "$SCRIPT_DIR/manifest.json" "$DEST/manifest.json"
cp "$SCRIPT_DIR/target/release/$EXECUTABLE" "$DEST/$EXECUTABLE"
chmod +x "$DEST/$EXECUTABLE"

echo "Installed to: $DEST"
