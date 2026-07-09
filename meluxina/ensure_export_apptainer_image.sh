#!/bin/bash

ensure_apptainer_image() {
  : "${HPO_IMAGE:?Set HPO_IMAGE to the Apptainer image path}"

  HPO_APPTAINER_MODULE=${HPO_APPTAINER_MODULE:-Apptainer/1.4.2-GCCcore-14.2.0}
  MELUXINA_AUTO_BUILD_IMAGE=${MELUXINA_AUTO_BUILD_IMAGE:-1}
  MELUXINA_IMAGE_SOURCE=${MELUXINA_IMAGE_SOURCE:-docker://pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime}
  MELUXINA_VERIFY_IMAGE=${MELUXINA_VERIFY_IMAGE:-1}

  if ! command -v module >/dev/null 2>&1; then
    printf 'The module command is unavailable. MeluXina modules are available inside compute jobs.\n' >&2
    return 2
  fi
  module load "${HPO_APPTAINER_MODULE}"

  NODE_TMP=${HPO_NODE_TMPDIR:-${TMPDIR:-/tmp}}
  export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${NODE_TMP%/}/apptainer-cache-${USER:-user}}
  export APPTAINER_TMPDIR=${APPTAINER_TMPDIR:-${NODE_TMP%/}/apptainer-tmp-${USER:-user}}
  mkdir -p "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}" "$(dirname -- "${HPO_IMAGE}")"

  if [[ -f "${HPO_IMAGE}" ]]; then
    printf 'Using Apptainer image: %s\n' "${HPO_IMAGE}"
  elif [[ "${MELUXINA_AUTO_BUILD_IMAGE}" == "1" || "${MELUXINA_AUTO_BUILD_IMAGE}" == "true" ]]; then
    tmp_image=${HPO_IMAGE}.tmp-${SLURM_JOB_ID:-$$}
    rm -f -- "${tmp_image}"
    printf 'Building Apptainer image: %s\n' "${HPO_IMAGE}"
    printf 'Apptainer image source: %s\n' "${MELUXINA_IMAGE_SOURCE}"
    if ! apptainer build "${tmp_image}" "${MELUXINA_IMAGE_SOURCE}"; then
      rm -f -- "${tmp_image}"
      return 1
    fi
    mv -f -- "${tmp_image}" "${HPO_IMAGE}"
  else
    printf 'Missing Apptainer image: %s\n' "${HPO_IMAGE}" >&2
    printf 'Set MELUXINA_AUTO_BUILD_IMAGE=1 or build the image manually.\n' >&2
    return 2
  fi

  if [[ "${MELUXINA_VERIFY_IMAGE}" == "1" || "${MELUXINA_VERIFY_IMAGE}" == "true" ]]; then
    apptainer exec "${HPO_IMAGE}" python -c 'import networkx, torch, yaml; print("Verified Apptainer image packages: networkx=%s torch=%s yaml=%s" % (networkx.__version__, torch.__version__, yaml.__version__))'
  fi
}
