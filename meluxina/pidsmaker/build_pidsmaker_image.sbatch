#!/bin/bash -l
#SBATCH --job-name=pids_image
#SBATCH --account=p201223
#SBATCH --partition=cpu
#SBATCH --qos=dev
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH --time=06:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ -z "${P_EDR_ROOT:-}" ]]; then
  if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/.git" ]]; then
    P_EDR_ROOT=$(cd -- "${SLURM_SUBMIT_DIR}" && pwd -P)
  else
    P_EDR_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)
  fi
fi

MELUXINA_PIDSMAKER_IMAGE=${MELUXINA_PIDSMAKER_IMAGE:-${P_EDR_ROOT}/containers/pidsmaker-pids.sif}
MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE=${MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE:-1}
MELUXINA_PIDSMAKER_VERIFY_IMAGE=${MELUXINA_PIDSMAKER_VERIFY_IMAGE:-1}

export P_EDR_ROOT
export MELUXINA_PIDSMAKER_IMAGE
export MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE
export MELUXINA_PIDSMAKER_VERIFY_IMAGE

source "${P_EDR_ROOT}/meluxina/pidsmaker/ensure_pidsmaker_apptainer_image.sh"
ensure_pidsmaker_apptainer_image
