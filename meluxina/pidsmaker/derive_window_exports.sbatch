#!/bin/bash -l
#SBATCH --job-name=pids_derive_windows
#SBATCH --account=p201223
#SBATCH --partition=cpu
#SBATCH --qos=default
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ -z "${P_EDR_ROOT:-}" ]]; then
  if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/.git" ]]; then
    P_EDR_ROOT=$(cd -- "${SLURM_SUBMIT_DIR}" && pwd -P)
  else
    P_EDR_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)
  fi
fi

MELUXINA_PIDSMAKER_IMAGE=${MELUXINA_PIDSMAKER_IMAGE:-${P_EDR_ROOT}/containers/pidsmaker-pids-psycopg2.sif}
MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE=${MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE:-0}
MELUXINA_PIDSMAKER_VERIFY_IMAGE=${MELUXINA_PIDSMAKER_VERIFY_IMAGE:-1}
SOURCE_ROOT=${PIDSMAKER_DERIVE_SOURCE_ROOT:-${P_EDR_ROOT}/capture_export/pidsmaker_export}
OUTPUT_BASE=${PIDSMAKER_DERIVE_OUTPUT_BASE:-${P_EDR_ROOT}/capture_export/pidsmaker_export_variants/recap_raw}
TOOL=${PIDSMAKER_DERIVE_TOOL:-recap}
WINDOWS=${PIDSMAKER_DERIVE_WINDOW_SIZE_SECONDS:-120,240}

export P_EDR_ROOT
export MELUXINA_PIDSMAKER_IMAGE
export MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE
export MELUXINA_PIDSMAKER_VERIFY_IMAGE

source "${P_EDR_ROOT}/meluxina/pidsmaker/ensure_pidsmaker_apptainer_image.sh"
ensure_pidsmaker_apptainer_image

NODE_TMP=${MELUXINA_PIDSMAKER_NODE_TMPDIR:-${TMPDIR:-/tmp}}
export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${NODE_TMP%/}/apptainer-cache-${USER:-user}}
export APPTAINER_TMPDIR=${APPTAINER_TMPDIR:-${NODE_TMP%/}/apptainer-tmp-${USER:-user}}
mkdir -p "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}" "${OUTPUT_BASE}"

printf 'P_EDR_ROOT=%s\n' "${P_EDR_ROOT}"
printf 'MELUXINA_PIDSMAKER_IMAGE=%s\n' "${MELUXINA_PIDSMAKER_IMAGE}"
printf 'SOURCE_ROOT=%s\n' "${SOURCE_ROOT}"
printf 'OUTPUT_BASE=%s\n' "${OUTPUT_BASE}"
printf 'TOOL=%s\n' "${TOOL}"
printf 'WINDOWS=%s\n' "${WINDOWS}"

WINDOWS_NORMALIZED=${WINDOWS//,/ }
WINDOWS_NORMALIZED=${WINDOWS_NORMALIZED//|/ }
for seconds in ${WINDOWS_NORMALIZED}; do
  if [[ "${seconds}" -lt 1 ]]; then
    printf 'Invalid window size seconds: %s\n' "${seconds}" >&2
    exit 2
  fi
  if (( seconds % 60 == 0 )); then
    label="$((seconds / 60))m"
  else
    label="${seconds}s"
  fi
  output_root=${OUTPUT_BASE%/}/window_${label}
  printf '\nDeriving %ss export at %s\n' "${seconds}" "${output_root}"
  apptainer exec \
    --no-mount home \
    --bind "${P_EDR_ROOT}:${P_EDR_ROOT}" \
    --pwd "${P_EDR_ROOT}" \
    "${MELUXINA_PIDSMAKER_IMAGE}" \
    python -u "${P_EDR_ROOT}/capture_export/pipeline/derive_pidsmaker_window_export.py" \
      --source-root "${SOURCE_ROOT}" \
      --output-root "${output_root}" \
      --tool "${TOOL}" \
      --window-size-seconds "${seconds}" \
      --replace
done
