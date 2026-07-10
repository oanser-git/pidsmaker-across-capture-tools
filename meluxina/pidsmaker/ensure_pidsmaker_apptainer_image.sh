#!/bin/bash

ensure_pidsmaker_apptainer_image() {
  : "${MELUXINA_PIDSMAKER_IMAGE:?Set MELUXINA_PIDSMAKER_IMAGE to the PIDSMaker Apptainer image path}"

  MELUXINA_PIDSMAKER_APPTAINER_MODULE=${MELUXINA_PIDSMAKER_APPTAINER_MODULE:-Apptainer/1.4.2-GCCcore-14.2.0}
  MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE=${MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE:-1}
  MELUXINA_PIDSMAKER_IMAGE_DEF=${MELUXINA_PIDSMAKER_IMAGE_DEF:-${P_EDR_ROOT}/meluxina/pidsmaker/pidsmaker-pids.def}
  MELUXINA_PIDSMAKER_VERIFY_IMAGE=${MELUXINA_PIDSMAKER_VERIFY_IMAGE:-1}
  MELUXINA_PIDSMAKER_IMAGE_WAIT_SECONDS=${MELUXINA_PIDSMAKER_IMAGE_WAIT_SECONDS:-21600}
  MELUXINA_PIDSMAKER_RETRY_FAKEROOT=${MELUXINA_PIDSMAKER_RETRY_FAKEROOT:-0}

  if ! command -v apptainer >/dev/null 2>&1; then
    if ! command -v module >/dev/null 2>&1; then
      printf 'The module command is unavailable. MeluXina modules are available inside compute jobs.\n' >&2
      return 2
    fi
    module load "${MELUXINA_PIDSMAKER_APPTAINER_MODULE}"
  fi

  NODE_TMP=${MELUXINA_PIDSMAKER_NODE_TMPDIR:-${TMPDIR:-/tmp}}
  export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${NODE_TMP%/}/apptainer-cache-${USER:-user}}
  export APPTAINER_TMPDIR=${APPTAINER_TMPDIR:-${NODE_TMP%/}/apptainer-tmp-${USER:-user}}
  mkdir -p "${APPTAINER_CACHEDIR}" "${APPTAINER_TMPDIR}" "$(dirname -- "${MELUXINA_PIDSMAKER_IMAGE}")"

  if [[ -f "${MELUXINA_PIDSMAKER_IMAGE}" ]]; then
    printf 'Using PIDSMaker Apptainer image: %s\n' "${MELUXINA_PIDSMAKER_IMAGE}"
  elif [[ "${MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE}" == "1" || "${MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE}" == "true" ]]; then
    if [[ ! -f "${MELUXINA_PIDSMAKER_IMAGE_DEF}" ]]; then
      printf 'Missing PIDSMaker Apptainer definition: %s\n' "${MELUXINA_PIDSMAKER_IMAGE_DEF}" >&2
      return 2
    fi

    lock_dir=${MELUXINA_PIDSMAKER_IMAGE}.lock
    if mkdir "${lock_dir}" 2>/dev/null; then
      tmp_image=${MELUXINA_PIDSMAKER_IMAGE}.tmp-${SLURM_JOB_ID:-$$}
      rm -f -- "${tmp_image}"
      trap 'rm -rf -- "${lock_dir}" "${tmp_image}"' RETURN

      printf 'Building PIDSMaker Apptainer image: %s\n' "${MELUXINA_PIDSMAKER_IMAGE}"
      printf 'Apptainer definition: %s\n' "${MELUXINA_PIDSMAKER_IMAGE_DEF}"
      if ! apptainer build ${MELUXINA_PIDSMAKER_BUILD_ARGS:-} "${tmp_image}" "${MELUXINA_PIDSMAKER_IMAGE_DEF}"; then
        if [[ "${MELUXINA_PIDSMAKER_RETRY_FAKEROOT}" == "1" || "${MELUXINA_PIDSMAKER_RETRY_FAKEROOT}" == "true" ]]; then
          printf 'Initial Apptainer build failed; retrying with --fakeroot.\n'
          rm -f -- "${tmp_image}"
          apptainer build --fakeroot ${MELUXINA_PIDSMAKER_BUILD_ARGS:-} "${tmp_image}" "${MELUXINA_PIDSMAKER_IMAGE_DEF}" || return 1
        else
          return 1
        fi
      fi
      mv -f -- "${tmp_image}" "${MELUXINA_PIDSMAKER_IMAGE}"
      rm -rf -- "${lock_dir}"
      trap - RETURN
    else
      printf 'Waiting for concurrent PIDSMaker image build lock: %s\n' "${lock_dir}"
      waited=0
      while [[ ! -f "${MELUXINA_PIDSMAKER_IMAGE}" && -d "${lock_dir}" && "${waited}" -lt "${MELUXINA_PIDSMAKER_IMAGE_WAIT_SECONDS}" ]]; do
        sleep 30
        waited=$((waited + 30))
      done
      if [[ ! -f "${MELUXINA_PIDSMAKER_IMAGE}" ]]; then
        printf 'Timed out waiting for PIDSMaker image: %s\n' "${MELUXINA_PIDSMAKER_IMAGE}" >&2
        return 1
      fi
    fi
  else
    printf 'Missing PIDSMaker Apptainer image: %s\n' "${MELUXINA_PIDSMAKER_IMAGE}" >&2
    printf 'Set MELUXINA_PIDSMAKER_AUTO_BUILD_IMAGE=1 or build the image manually.\n' >&2
    return 2
  fi

  if [[ "${MELUXINA_PIDSMAKER_VERIFY_IMAGE}" == "1" || "${MELUXINA_PIDSMAKER_VERIFY_IMAGE}" == "true" ]]; then
    apptainer exec "${MELUXINA_PIDSMAKER_IMAGE}" python - <<'PY'
import networkx
import pandas
import sklearn
import torch
import torch_geometric
import yaml
import yacs
print("Verified PIDSMaker image packages: torch={} pyg={} networkx={} pandas={}".format(
    torch.__version__, torch_geometric.__version__, networkx.__version__, pandas.__version__
))
PY
  fi
}
