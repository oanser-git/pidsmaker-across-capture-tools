#!/bin/bash -l
#SBATCH --job-name=recap_raw100_asha_ctl
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3-00:00:00
#SBATCH --array=0-3

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)

export P_EDR_ROOT=${P_EDR_ROOT:-${REPO_ROOT}}
export ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT:-${P_EDR_ROOT}/capture_export/pidsmaker_export}

: "${HPO_IMAGE:?Set HPO_IMAGE so ASHA-submitted HPO jobs can run}"

IFS=' ' read -r -a METHODS <<< "${HPO_ASHA_METHODS:-magic velox orthrus kairos}"
TASK_ID=${SLURM_ARRAY_TASK_ID:-0}
if (( TASK_ID < 0 || TASK_ID >= ${#METHODS[@]} )); then
  printf 'SLURM_ARRAY_TASK_ID=%s is outside HPO_ASHA_METHODS length %s\n' "${TASK_ID}" "${#METHODS[@]}" >&2
  exit 2
fi

METHOD=${HPO_ASHA_METHOD:-${METHODS[${TASK_ID}]}}
CONFIG=${HPO_ASHA_CONFIG:-${P_EDR_ROOT}/pidsmaker_experiments/experiments/asha/${METHOD}_recap_raw_100_asha.yml}

ARGS=(--config "${CONFIG}")
if [[ "${HPO_ASHA_ONCE:-0}" == "1" ]]; then
  ARGS+=(--once)
fi
if [[ "${HPO_ASHA_DRY_RUN:-0}" == "1" ]]; then
  ARGS+=(--dry-run)
fi
if [[ -n "${HPO_ASHA_POLL_SECONDS:-}" ]]; then
  ARGS+=(--poll-seconds "${HPO_ASHA_POLL_SECONDS}")
fi
if [[ -n "${HPO_ASHA_MAX_LOOPS:-}" ]]; then
  ARGS+=(--max-loops "${HPO_ASHA_MAX_LOOPS}")
fi

mkdir -p "${P_EDR_ROOT}/run_logs"

P_EDR_PYTHON=${P_EDR_PYTHON:-python3}
"${P_EDR_PYTHON}" "${P_EDR_ROOT}/pidsmaker_experiments/scripts/run_asha_slurm.py" "${ARGS[@]}"
