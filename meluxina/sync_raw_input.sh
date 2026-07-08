#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
LOCAL_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd -P)

MELUXINA_USER=${MELUXINA_USER:-u101059}
MELUXINA_HOST=${MELUXINA_HOST:-login.lxp.lu}
MELUXINA_PORT=${MELUXINA_PORT:-8822}
MELUXINA_SSH_KEY=${MELUXINA_SSH_KEY:-${HOME}/.ssh/meluxina}
MELUXINA_PROJECT=${MELUXINA_PROJECT:-p201223}

LOCAL_RAW_DIR=${LOCAL_RAW_DIR:-${LOCAL_ROOT}/capture_export/data-raw}
REMOTE_ROOT=${REMOTE_ROOT:-}
REMOTE_RAW_DIR=${REMOTE_RAW_DIR:-}
DELETE_REMOTE=0
DRY_RUN=0

usage() {
  printf 'Usage: %s [--project PROJECT] [--local-raw DIR] [--remote-root DIR] [--remote-raw DIR] [--delete] [--dry-run]\n' "$0" >&2
}

remote_quote() {
  printf '%q' "$1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      MELUXINA_PROJECT=$2
      shift 2
      ;;
    --local-raw)
      LOCAL_RAW_DIR=$2
      shift 2
      ;;
    --remote-root)
      REMOTE_ROOT=$2
      shift 2
      ;;
    --remote-raw)
      REMOTE_RAW_DIR=$2
      shift 2
      ;;
    --delete)
      DELETE_REMOTE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

REMOTE_ROOT=${REMOTE_ROOT:-/project/home/${MELUXINA_PROJECT}/pidsmaker-across-capture-tools}
REMOTE_RAW_DIR=${REMOTE_RAW_DIR:-${REMOTE_ROOT}/capture_export/data-raw}

if [[ ! -d "${LOCAL_RAW_DIR}" ]]; then
  printf 'Local raw directory does not exist: %s\n' "${LOCAL_RAW_DIR}" >&2
  exit 2
fi
if [[ ! -f "${MELUXINA_SSH_KEY}" ]]; then
  printf 'SSH key does not exist: %s\n' "${MELUXINA_SSH_KEY}" >&2
  exit 2
fi

SSH=(ssh -i "${MELUXINA_SSH_KEY}" -p "${MELUXINA_PORT}" "${MELUXINA_USER}@${MELUXINA_HOST}")
remote_git=$(remote_quote "${REMOTE_ROOT}/.git")
remote_raw=$(remote_quote "${REMOTE_RAW_DIR}")

"${SSH[@]}" "test -d ${remote_git} || { printf 'Remote repo is missing: %s\\n' ${remote_git} >&2; exit 2; }; mkdir -p ${remote_raw}"

RSYNC_RSH="ssh -i ${MELUXINA_SSH_KEY} -p ${MELUXINA_PORT}"
RSYNC_ARGS=(-a --info=progress2)
if [[ "${DELETE_REMOTE}" == "1" ]]; then
  RSYNC_ARGS+=(--delete)
fi
if [[ "${DRY_RUN}" == "1" ]]; then
  RSYNC_ARGS+=(--dry-run)
fi

rsync "${RSYNC_ARGS[@]}" \
  -e "${RSYNC_RSH}" \
  "${LOCAL_RAW_DIR%/}/" \
  "${MELUXINA_USER}@${MELUXINA_HOST}:${REMOTE_RAW_DIR%/}/"
