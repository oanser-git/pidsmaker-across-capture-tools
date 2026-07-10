#!/bin/bash -l
#SBATCH --job-name=prepare_raw_export
#SBATCH --account=p201223
#SBATCH --partition=cpu
#SBATCH --qos=default
#SBATCH --cpus-per-task=256
#SBATCH --mem=480G
#SBATCH --time=2-00:00:00

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
if [[ -z "${P_EDR_ROOT:-}" ]]; then
  if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/.git" ]]; then
    P_EDR_ROOT=$(cd -- "${SLURM_SUBMIT_DIR}" && pwd -P)
  else
    P_EDR_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd -P)
  fi
fi
HPO_APPTAINER_MODULE=${HPO_APPTAINER_MODULE:-Apptainer/1.4.2-GCCcore-14.2.0}
HPO_IMAGE=${HPO_IMAGE:-${P_EDR_ROOT}/containers/export-prep.sif}
MELUXINA_IMAGE_SOURCE=${MELUXINA_IMAGE_SOURCE:-docker://pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime}
MELUXINA_AUTO_BUILD_IMAGE=${MELUXINA_AUTO_BUILD_IMAGE:-1}
MELUXINA_EXPORT_WORKERS=${MELUXINA_EXPORT_WORKERS:-16}
PIDSMAKER_EXPORT_WINDOW_SIZE_SECONDS=${PIDSMAKER_EXPORT_WINDOW_SIZE_SECONDS:-60}

DATA_RAW=${DATA_RAW:-${P_EDR_ROOT}/capture_export/data-raw}
REFERENCE_DATASET=${REFERENCE_DATASET:-${P_EDR_ROOT}/capture_export/reference_dataset}
PIDSMAKER_EXPORT=${PIDSMAKER_EXPORT:-${P_EDR_ROOT}/capture_export/pidsmaker_export}
LOG_DIR=${LOG_DIR:-${P_EDR_ROOT}/run_logs}

if [[ ! -d "${P_EDR_ROOT}/.git" ]]; then
  printf 'P_EDR_ROOT does not look like the repo root: %s\n' "${P_EDR_ROOT}" >&2
  exit 2
fi
if [[ ! -d "${DATA_RAW}" ]]; then
  printf 'Missing raw input directory: %s\n' "${DATA_RAW}" >&2
  exit 2
fi
if ! compgen -G "${DATA_RAW%/}/*" >/dev/null; then
  printf 'Raw input directory is empty: %s\n' "${DATA_RAW}" >&2
  exit 2
fi

mkdir -p "${LOG_DIR}" "${REFERENCE_DATASET}" "${PIDSMAKER_EXPORT}"

TOOLS_YAML=""
if [[ -n "${MELUXINA_EXPORT_TOOLS:-}" ]]; then
  TOOLS_YAML="  tools:"
  IFS=',' read -ra EXPORT_TOOLS <<< "${MELUXINA_EXPORT_TOOLS}"
  for TOOL in "${EXPORT_TOOLS[@]}"; do
    TOOL=$(printf '%s' "${TOOL}" | xargs)
    if [[ -n "${TOOL}" ]]; then
      TOOLS_YAML+=$'\n'"    - ${TOOL}"
    fi
  done
fi

CONFIG_PATH=${LOG_DIR}/meluxina_pipeline_config_${SLURM_JOB_ID:-manual}.yml
cat > "${CONFIG_PATH}" <<EOF
paths:
  data_raw: ${DATA_RAW}
  reference_dataset: ${REFERENCE_DATASET}
  pidsmaker_export: ${PIDSMAKER_EXPORT}

processing:
  workers: ${MELUXINA_EXPORT_WORKERS}

pidsmaker_export:
  window_size_seconds: ${PIDSMAKER_EXPORT_WINDOW_SIZE_SECONDS}
${TOOLS_YAML}

steps:
  build_reference_dataset: true
  build_splits: true
  export_to_pidsmaker: true

splits:
  validation_has_attacks: false
  benign_ratios:
    train: 0.70
    val: 0.15
    test: 0.15
EOF

export MELUXINA_PIPELINE_CONFIG=${CONFIG_PATH}
export PYTHONPATH=${P_EDR_ROOT}${PYTHONPATH:+:${PYTHONPATH}}

printf 'P_EDR_ROOT=%s\n' "${P_EDR_ROOT}"
printf 'DATA_RAW=%s\n' "${DATA_RAW}"
printf 'REFERENCE_DATASET=%s\n' "${REFERENCE_DATASET}"
printf 'PIDSMAKER_EXPORT=%s\n' "${PIDSMAKER_EXPORT}"
printf 'HPO_IMAGE=%s\n' "${HPO_IMAGE}"
printf 'MELUXINA_IMAGE_SOURCE=%s\n' "${MELUXINA_IMAGE_SOURCE}"
printf 'MELUXINA_AUTO_BUILD_IMAGE=%s\n' "${MELUXINA_AUTO_BUILD_IMAGE}"
printf 'MELUXINA_EXPORT_WORKERS=%s\n' "${MELUXINA_EXPORT_WORKERS}"
printf 'PIDSMAKER_EXPORT_WINDOW_SIZE_SECONDS=%s\n' "${PIDSMAKER_EXPORT_WINDOW_SIZE_SECONDS}"
printf 'MELUXINA_EXPORT_TOOLS=%s\n' "${MELUXINA_EXPORT_TOOLS:-all}"
printf 'MELUXINA_PIPELINE_CONFIG=%s\n' "${MELUXINA_PIPELINE_CONFIG}"

source "${SCRIPT_DIR}/ensure_export_apptainer_image.sh"
ensure_apptainer_image

export APPTAINERENV_MELUXINA_PIPELINE_CONFIG=${MELUXINA_PIPELINE_CONFIG}
export APPTAINERENV_PYTHONPATH=${PYTHONPATH}

RUNNER=(
  apptainer exec
  --bind "${P_EDR_ROOT}:${P_EDR_ROOT}"
  --pwd "${P_EDR_ROOT}"
  "${HPO_IMAGE}"
  python
)

"${RUNNER[@]}" -c 'import os; from capture_export.pipeline.run_pipeline import main; main(os.environ["MELUXINA_PIPELINE_CONFIG"])'
