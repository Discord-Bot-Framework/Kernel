#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly CONFIGS_DIR="${SCRIPT_DIR}/../Discord"
readonly DOTENV_PATH="${CONFIGS_DIR}/.env"
readonly VENV_DIR="${SCRIPT_DIR}/.venv"
readonly REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements.txt"
readonly PROFILE_FILE="${SCRIPT_DIR}/firejail.profile"

cd "${SCRIPT_DIR}"

if [[ -f .env ]]; then
  mkdir -p "${CONFIGS_DIR}"
  cp .env "${DOTENV_PATH}"
fi

if [[ -f "${DOTENV_PATH}" ]]; then
  cp "${DOTENV_PATH}" .env
fi

if ! command -v python3 >/dev/null 2>&1; then
  printf 'Not found: Python\n' >&2
  exit 1
fi

# if ! command -v bun >/dev/null 2>&1; then
#   printf 'Not found: Bun\n' >&2
# fi

if [[ ! -d "${VENV_DIR}" ]]; then
  python3 -m venv "${VENV_DIR}"
fi

if [[ -f "${REQUIREMENTS_FILE}" ]]; then
  "${VENV_DIR}/bin/pip" install -q -r "${REQUIREMENTS_FILE}"
fi

if [[ ! -f "${PROFILE_FILE}" ]]; then
  printf 'Not found: firejail.profile\n' >&2
  exit 1
fi

exec firejail \
  --profile="${PROFILE_FILE}" \
  --read-write="${SCRIPT_DIR}" \
  --read-only="${PROFILE_FILE}" \
  "${VENV_DIR}/bin/python" -OO main.py
