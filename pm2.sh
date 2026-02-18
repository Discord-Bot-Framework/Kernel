#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly ECOSYSTEM_FILE="${SCRIPT_DIR}/ecosystem.config.js"

if [[ ! -f "${ECOSYSTEM_FILE}" ]]; then
  printf 'Not found: ecosystem.config.js at %s\n' "${SCRIPT_DIR}" >&2
  exit 1
fi

if ! command -v pm2 >/dev/null 2>&1; then
  printf 'Not found: pm2\n' >&2
  exit 1
fi

cd "${SCRIPT_DIR}"

if [[ $# -eq 0 ]]; then
  set -- start
fi

pm2 "$@" "${ECOSYSTEM_FILE}"
