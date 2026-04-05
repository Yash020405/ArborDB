#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() {
  echo "[bootstrap] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' is not installed or not in PATH." >&2
    exit 1
  fi
}

build_engine() {
  require_cmd g++
  log "Building native engine"
  pushd "$ROOT_DIR/engine" >/dev/null
  mkdir -p build
  g++ -std=c++17 -O2 \
    -I./vendor -I./src \
    main.cpp \
    src/engine.cpp \
    src/wal.cpp \
    src/disk/pager.cpp \
    src/schema/schema.cpp \
    src/storage/btree.cpp \
    src/storage/secondary_index.cpp \
    src/storage/serializer.cpp \
    src/storage/table_store.cpp \
    -o build/engine
  popd >/dev/null
}

install_dependencies() {
  require_cmd npm
  log "Installing API dependencies"
  (cd "$ROOT_DIR/api" && npm install)

  log "Installing frontend dependencies"
  (cd "$ROOT_DIR/frontend" && npm install)

  log "Installing CLI dependencies"
  (cd "$ROOT_DIR/cli" && npm install)
}

run_checks() {
  log "Running API lint"
  (cd "$ROOT_DIR/api" && npm run lint)

  log "Running CLI lint"
  (cd "$ROOT_DIR/cli" && npm run lint)

  log "Running frontend lint"
  (cd "$ROOT_DIR/frontend" && npm run lint)

  log "Running API tests"
  (cd "$ROOT_DIR/api" && npm test -- --runInBand)

  log "Building frontend"
  (cd "$ROOT_DIR/frontend" && npm run build)

  build_engine
  log "All checks completed"
}

start_api() {
  log "Starting API"
  (cd "$ROOT_DIR/api" && npm start)
}

start_frontend() {
  log "Starting frontend"
  (cd "$ROOT_DIR/frontend" && npm run dev)
}

start_cli() {
  log "Starting CLI"
  (cd "$ROOT_DIR/cli" && node arbordb.js)
}

start_up() {
  build_engine
  install_dependencies

  log "Starting API in background"
  (cd "$ROOT_DIR/api" && npm start) &
  api_pid=$!

  cleanup() {
    if kill -0 "$api_pid" >/dev/null 2>&1; then
      log "Stopping background API"
      kill "$api_pid" >/dev/null 2>&1 || true
    fi
  }

  trap cleanup EXIT INT TERM

  log "Starting frontend (foreground)"
  log "Use a second terminal for CLI: cd cli && node arbordb.js"
  (cd "$ROOT_DIR/frontend" && npm run dev)
}

usage() {
  cat <<'EOF'
Usage: scripts/bootstrap.sh <command>

Commands:
  setup      Build native engine and install all dependencies
  check      Run lint/tests/build checks across API, CLI, frontend, and engine
  up         setup + start API (background) and frontend (foreground)
  api        Start API only
  frontend   Start frontend only
  cli        Start CLI only
  help       Show this help text
EOF
}

main() {
  command="${1:-help}"

  case "$command" in
    setup)
      build_engine
      install_dependencies
      ;;
    check)
      run_checks
      ;;
    up)
      start_up
      ;;
    api)
      start_api
      ;;
    frontend)
      start_frontend
      ;;
    cli)
      start_cli
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      echo "Unknown command: $command" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
