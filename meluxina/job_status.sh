#!/usr/bin/env bash
set -euo pipefail

REPO=${MELUXINA_REPO:-/project/home/p201223/pidsmaker-across-capture-tools}
SSH_HOST=${MELUXINA_SSH_HOST:-u101059@login.lxp.lu}
SSH_KEY=${MELUXINA_SSH_KEY:-/home/omar/.ssh/meluxina}
SSH_PORT=${MELUXINA_SSH_PORT:-8822}
LINES=${MELUXINA_STATUS_LINES:-40}

r() { ssh -n -i "$SSH_KEY" -p "$SSH_PORT" "$SSH_HOST" "$1"; }
s() { printf '\n== %s ==\n' "$1"; }

memo() {
  cat <<'EOF'
MeluXina memo:
  Accounts you can submit with: p201223, p201219.
  Both accounts expose the same Slurm access for this user: cpu/gpu/fpga/largemem + QoS below.
  Account submit limit visible in Slurm: up to 100 submitted jobs per account.
  Use /mnt/tier2/project/<account>/ for that account's project storage.
  Partitions: cpu = 256 CPU / 480G nodes, gpu = 128 CPU / 480G / 4 GPU nodes,
              largemem = 256 CPU / ~4TB nodes, fpga = 128 CPU / 480G / FPGA nodes.
  QoS you can use: dev = 6h quick jobs, short = 6h,
                   short-preempt = 6h but preemptible,
                   default = 2 days, long = 6 days, test = 30 min.
  QoS max job size: dev = 1 CPU node or 1 GPU node or 1 largemem node;
                    short/short-preempt/long/test = up to 28 CPU nodes or 40 GPUs;
                    default = up to 140 CPU nodes or 200 GPUs.
  QoS max running jobs: dev/long/test = 1 job; short/default have no explicit max-job limit shown.
  Not useful here: bench partition is down; normal/urgent QoS are not allowed for this user.
EOF
}

pick_job() {
  local i id part name state elapsed left nodes reason
  mapfile -t jobs < <(r 'squeue -u "$USER" -h -o "%i|%P|%j|%T|%M|%L|%D|%R" | sort -t "|" -k1,1nr')
  ((${#jobs[@]})) || { printf 'No queued or running MeluXina jobs found.\n'; exit 0; }
  labels=()
  for i in "${!jobs[@]}"; do
    IFS='|' read -r id part name state elapsed left nodes reason <<< "${jobs[$i]}"
    labels+=("job=$id partition=$part name=$name state=$state elapsed=$elapsed remaining=$left nodes=$nodes node/reason=$reason")
  done
  printf '\n'
  memo
  printf '\nCurrent MeluXina jobs:\n'
  PS3='Select job number: '
  select label in "${labels[@]}" Quit; do
    [[ ${label:-} == Quit ]] && exit 0
    [[ -n ${label:-} ]] && break
    printf 'Invalid selection.\n'
  done
  IFS='|' read -r JOB_ID PART JOB_NAME STATE ELAPSED LEFT NODES REASON <<< "${jobs[$((REPLY - 1))]}"
  OUT=${REPO%/}/run_logs/${JOB_NAME}-${JOB_ID}.out
  ERR=${OUT%.out}.err
}

progress() { r "if [ -f '$OUT' ]; then grep -E '\\[[0-9]+/[0-9]+\\]|completed|wrote|Wrote|Error|error|Traceback|MemoryError' '$OUT' | tail -n '$LINES'; else printf 'Missing stdout log: %s\\n' '$OUT'; fi"; }
logs() { r "if [ -f '$OUT' ]; then tail -n '$LINES' '$OUT'; else printf 'Missing stdout log: %s\\n' '$OUT'; fi"; }
errors() { r "if [ -s '$ERR' ]; then tail -n '$LINES' '$ERR'; else printf 'stderr log is empty or missing: %s\\n' '$ERR'; fi"; }
size() { r "for p in capture_export/pidsmaker_export capture_export/reference_dataset run_logs; do full='${REPO%/}'/\$p; [ -e \"\$full\" ] && du -sh \"\$full\"; done; df -h '$REPO' | sed -n '1p;2p'"; }
memory() {
  node=$(r "squeue -j '$JOB_ID' -h -o '%N'")
  [[ -n $node && $node != '(None)' ]] || { printf 'Job is not running; no live memory available.\n'; return; }
  req_mem=$(r "squeue -j '$JOB_ID' -h -o '%m'")
  case "$req_mem" in *T) req_gib=$(( ${req_mem%T} * 1024 ));; *G) req_gib=${req_mem%G};; *M) req_gib=$(( ${req_mem%M} / 1024 ));; *) req_gib=0;; esac
  r "ssh '$node' \"ps -u \\\"\$USER\\\" -o pid=,ppid=,pcpu=,rss=,etime=,comm= --sort=-rss; ps -u \\\"\$USER\\\" -o pid=,comm= | awk '\\$2==\\\"python\\\"{print \\$1}' | while read -r pid; do awk '/^Pss:/{pss+=\\$2} END{print pss+0}' /proc/\\$pid/smaps_rollup 2>/dev/null; done | awk -v limit=$req_gib 'BEGIN{pss=0} {pss+=\\$1} END{pss/=1048576; printf \\\"python_pss_gib=%.1f/%.1f (%.0f%% of requested memory, PSS)\\\\n\\\", pss, limit, (limit>0?pss*100/limit:0)}'\""
}
while true; do
  pick_job
  while true; do
    printf '\nSelected %s (%s).\n' "$JOB_ID" "$JOB_NAME"
    PS3='Select view: '
    select view in Full Progress Logs Errors Memory Size 'Other job' Quit; do
      case ${view:-} in
        Full) s Queue; r "squeue -j '$JOB_ID' -o '%.18i %.9P %.24j %.2t %.12M %.12L %.6D %R'"; s Progress; progress; s Errors; errors; s Size; size; s Memory; memory; break ;;
        Progress) s Progress; progress; break ;;
        Logs) s Logs; logs; break ;;
        Errors) s Errors; errors; break ;;
        Memory) s Memory; memory; break ;;
        Size) s Size; size; break ;;
        'Other job') break 2 ;;
        Quit) exit 0 ;;
        *) printf 'Invalid selection.\n' ;;
      esac
    done
  done
done
