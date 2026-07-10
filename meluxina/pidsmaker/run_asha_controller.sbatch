#!/bin/bash -l
#SBATCH --job-name=pids_asha
#SBATCH --account=p201223
#SBATCH --partition=cpu
#SBATCH --qos=default
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=2-00:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ -z "${P_EDR_ROOT:-}" ]]; then
  if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/.git" ]]; then
    P_EDR_ROOT=$(cd -- "${SLURM_SUBMIT_DIR}" && pwd -P)
  else
    P_EDR_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)
  fi
fi

: "${MELUXINA_PIDSMAKER_ASHA_CONFIG:?Set MELUXINA_PIDSMAKER_ASHA_CONFIG to an ASHA YAML path}"

export P_EDR_ROOT
export ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT:-/mnt/tier2/project/p201223/pidsmaker-across-capture-tools/capture_export/pidsmaker_export}

ARGS=(--config "${MELUXINA_PIDSMAKER_ASHA_CONFIG}")
if [[ -n "${MELUXINA_PIDSMAKER_ASHA_POLL_SECONDS:-}" ]]; then
  ARGS+=(--poll-seconds "${MELUXINA_PIDSMAKER_ASHA_POLL_SECONDS}")
fi
if [[ -n "${MELUXINA_PIDSMAKER_ASHA_MAX_LOOPS:-}" ]]; then
  ARGS+=(--max-loops "${MELUXINA_PIDSMAKER_ASHA_MAX_LOOPS}")
fi
if [[ "${MELUXINA_PIDSMAKER_ASHA_ONCE:-0}" == "1" ]]; then
  ARGS+=(--once)
fi
if [[ "${MELUXINA_PIDSMAKER_ASHA_DRY_RUN:-0}" == "1" ]]; then
  ARGS+=(--dry-run)
fi

P_EDR_PYTHON=${P_EDR_PYTHON:-python3}
exec "${P_EDR_PYTHON}" "${P_EDR_ROOT}/meluxina/pidsmaker/run_asha_slurm.py" "${ARGS[@]}"
