#!/bin/bash -l
#SBATCH --job-name=cross_read_smoke

set -euo pipefail

SOURCE_ROOT=${SMOKE_SOURCE_ROOT:-/mnt/tier2/project/p201223/pidsmaker-across-capture-tools}
INDEX=${SOURCE_ROOT}/capture_export/reference_dataset/dataset_index.json
EXPORT_DIR=${SOURCE_ROOT}/capture_export/pidsmaker_export

printf 'user=%s\n' "${USER}"
printf 'slurm_account=%s\n' "${SLURM_JOB_ACCOUNT:-unknown}"
printf 'source_root=%s\n' "${SOURCE_ROOT}"

printf '\n== Path Checks ==\n'
ls -ld "${SOURCE_ROOT}" "${SOURCE_ROOT}/capture_export" "${EXPORT_DIR}"
test -r "${INDEX}"
test -d "${EXPORT_DIR}/camflow"

printf '\n== Read Checks ==\n'
stat -c 'index=%n size=%s bytes' "${INDEX}"
dd if="${INDEX}" of=/dev/null bs=4096 count=1 status=none
printf 'read_first_4k_from_index=ok\n'
ls -1 "${EXPORT_DIR}" | sort

printf '\nCross-account read smoke test: OK\n'
