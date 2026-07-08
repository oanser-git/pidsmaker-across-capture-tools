#!/bin/bash -l
#SBATCH --job-name=pids_hpo
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=01:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)

export P_EDR_ROOT=${P_EDR_ROOT:-${REPO_ROOT}}
export ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT:-${P_EDR_ROOT}/capture_export/pidsmaker_export}
export HPO_SKIP_EXISTING=${HPO_SKIP_EXISTING:-1}

: "${HPO_SWEEP:?Set HPO_SWEEP to the sweep YAML path}"
: "${HPO_IMAGE:?Set HPO_IMAGE to the PIDSMaker Apptainer image}"

if [[ -n "${HPO_APPTAINER_MODULE:-}" ]]; then
  if ! command -v module >/dev/null 2>&1; then
    printf 'HPO_APPTAINER_MODULE is set, but the module command is unavailable\n' >&2
    exit 2
  fi
  module load "${HPO_APPTAINER_MODULE}"
fi

NODE_TMP=${HPO_NODE_TMPDIR:-${TMPDIR:-/tmp}}
export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${NODE_TMP%/}/apptainer-cache-${USER:-user}}
export APPTAINER_TMPDIR=${APPTAINER_TMPDIR:-${NODE_TMP%/}/apptainer-tmp-${USER:-user}}
export APPTAINERENV_ORANGE_EXPORT_ROOT=${ORANGE_EXPORT_ROOT}
export APPTAINERENV_PYTHONUNBUFFERED=1
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-${OMP_NUM_THREADS:-1}}

mkdir -p "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}" "${P_EDR_ROOT}/run_logs"

ARGS=(--sweep "${HPO_SWEEP}")
if [[ -n "${HPO_TAG:-}" ]]; then
  ARGS+=(--tag "${HPO_TAG}")
fi
if [[ -n "${HPO_INDEX:-}" ]]; then
  ARGS+=(--index "${HPO_INDEX}")
fi
if [[ -n "${HPO_RUN_NAME:-}" ]]; then
  ARGS+=(--run-name "${HPO_RUN_NAME}")
fi
if [[ "${HPO_RUN_TEST:-0}" == "1" ]]; then
  ARGS+=(--run-test)
fi
if [[ -n "${HPO_PHASE:-}" ]]; then
  ARGS+=(--phase "${HPO_PHASE}")
fi

P_EDR_PYTHON=${P_EDR_PYTHON:-python3}
"${P_EDR_PYTHON}" "${P_EDR_ROOT}/pidsmaker_experiments/scripts/run_hpo_apptainer_task.py" "${ARGS[@]}"
