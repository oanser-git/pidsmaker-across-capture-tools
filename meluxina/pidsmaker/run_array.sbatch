#!/bin/bash -l
#SBATCH --job-name=pids_native
#SBATCH --account=p201223
#SBATCH --partition=gpu
#SBATCH --qos=default
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=7
#SBATCH --mem=180G
#SBATCH --time=04:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ -z "${P_EDR_ROOT:-}" ]]; then
  if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/.git" ]]; then
    P_EDR_ROOT=$(cd -- "${SLURM_SUBMIT_DIR}" && pwd -P)
  else
    P_EDR_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)
  fi
fi

MELUXINA_PIDSMAKER_APPTAINER_MODULE=${MELUXINA_PIDSMAKER_APPTAINER_MODULE:-Apptainer/1.4.2-GCCcore-14.2.0}
MELUXINA_PIDSMAKER_IMAGE=${MELUXINA_PIDSMAKER_IMAGE:-${HPO_IMAGE:-${P_EDR_ROOT}/containers/pidsmaker-pids.sif}}
ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT:-/mnt/tier2/project/p201223/pidsmaker-across-capture-tools/capture_export/pidsmaker_export}
LOG_DIR=${LOG_DIR:-${P_EDR_ROOT}/run_logs}

: "${MELUXINA_PIDSMAKER_SWEEP:?Set MELUXINA_PIDSMAKER_SWEEP to a sweep YAML path}"

if [[ ! -d "${P_EDR_ROOT}/.git" ]]; then
  printf 'P_EDR_ROOT does not look like the repo root: %s\n' "${P_EDR_ROOT}" >&2
  exit 2
fi
if [[ ! -f "${ORANGE_EXPORT_ROOT}/export_manifest.json" ]]; then
  printf 'Missing Orange export manifest: %s\n' "${ORANGE_EXPORT_ROOT}/export_manifest.json" >&2
  exit 2
fi
if [[ ! -f "${MELUXINA_PIDSMAKER_IMAGE}" ]]; then
  printf 'Missing PIDSMaker Apptainer image: %s\n' "${MELUXINA_PIDSMAKER_IMAGE}" >&2
  exit 2
fi

if [[ -n "${MELUXINA_PIDSMAKER_APPTAINER_MODULE:-}" ]]; then
  if ! command -v module >/dev/null 2>&1; then
    printf 'MELUXINA_PIDSMAKER_APPTAINER_MODULE is set, but module is unavailable\n' >&2
    exit 2
  fi
  module load "${MELUXINA_PIDSMAKER_APPTAINER_MODULE}"
fi

NODE_TMP=${MELUXINA_PIDSMAKER_NODE_TMPDIR:-${TMPDIR:-/tmp}}
export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${NODE_TMP%/}/apptainer-cache-${USER:-user}}
export APPTAINER_TMPDIR=${APPTAINER_TMPDIR:-${NODE_TMP%/}/apptainer-tmp-${USER:-user}}
export ORANGE_EXPORT_ROOT
export P_EDR_ROOT
export MELUXINA_PIDSMAKER_IMAGE
export APPTAINERENV_ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT}
export APPTAINERENV_PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-${OMP_NUM_THREADS:-1}}

mkdir -p "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}" "${LOG_DIR}"

printf 'P_EDR_ROOT=%s\n' "${P_EDR_ROOT}"
printf 'MELUXINA_PIDSMAKER_SWEEP=%s\n' "${MELUXINA_PIDSMAKER_SWEEP}"
printf 'MELUXINA_PIDSMAKER_IMAGE=%s\n' "${MELUXINA_PIDSMAKER_IMAGE}"
printf 'ORANGE_EXPORT_ROOT=%s\n' "${ORANGE_EXPORT_ROOT}"
printf 'SLURM_ARRAY_TASK_ID=%s\n' "${SLURM_ARRAY_TASK_ID:-0}"

ARGS=(--sweep "${MELUXINA_PIDSMAKER_SWEEP}")
if [[ -n "${MELUXINA_PIDSMAKER_TAG:-}" ]]; then
  ARGS+=(--tag "${MELUXINA_PIDSMAKER_TAG}")
fi
if [[ -n "${MELUXINA_PIDSMAKER_INDEX:-}" ]]; then
  ARGS+=(--index "${MELUXINA_PIDSMAKER_INDEX}")
fi
if [[ -n "${MELUXINA_PIDSMAKER_RUN_NAME:-}" ]]; then
  ARGS+=(--run-name "${MELUXINA_PIDSMAKER_RUN_NAME}")
fi
if [[ -n "${MELUXINA_PIDSMAKER_PHASE:-}" ]]; then
  ARGS+=(--phase "${MELUXINA_PIDSMAKER_PHASE}")
fi

P_EDR_PYTHON=${P_EDR_PYTHON:-python3}
"${P_EDR_PYTHON}" "${P_EDR_ROOT}/meluxina/pidsmaker/run_apptainer_task.py" "${ARGS[@]}"
