#!/usr/bin/env bash
set -euo pipefail

REPO=${MELUXINA_REPO:-/mnt/tier2/project/p201223/pidsmaker-across-capture-tools}
SSH_HOST=${MELUXINA_SSH_HOST:-u101059@login.lxp.lu}
SSH_KEY=${MELUXINA_SSH_KEY:-/home/omar/.ssh/meluxina}
SSH_PORT=${MELUXINA_SSH_PORT:-8822}
LINES=${MELUXINA_STATUS_LINES:-40}

r() { ssh -n -i "$SSH_KEY" -p "$SSH_PORT" "$SSH_HOST" "$1"; }
s() { printf '\n== %s ==\n' "$1"; }

asha_summary() {
  r "REPO='${REPO%/}' python3 - <<'PY'
import json
import os
import subprocess
from collections import Counter
from pathlib import Path

root = Path(os.environ['REPO'])
methods = ['magic', 'velox', 'orthrus', 'kairos']
controller_names = {method: 'pids_asha_' + method for method in methods}

def array_task_count(job_id):
    if '_[' not in job_id:
        return 1
    try:
        inside = job_id.split('_[', 1)[1].rsplit(']', 1)[0].split('%', 1)[0]
        total = 0
        for segment in inside.split(','):
            if not segment:
                continue
            if '-' in segment:
                start, end = segment.split('-', 1)
                total += int(end) - int(start) + 1
            else:
                total += 1
        return max(total, 1)
    except Exception:
        return 1

def slurm_counts_text(counter):
    parts = []
    for state in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING']:
        count = counter[state]
        if count:
            parts.append('{}={}'.format(state, count))
    return ' '.join(parts) if parts else 'none'

def hpo_label(name):
    for method in methods:
        prefix = method + '_recap_raw100_'
        if name.startswith(prefix):
            token = name[len(prefix):].split('_', 1)[0]
            return method, token
    return None, None

queue_lines = subprocess.run(
    ['squeue', '-u', os.environ.get('USER', ''), '-h', '-o', '%.120i|%.80j|%T|%M|%a|%R'],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    universal_newlines=True,
).stdout.splitlines()

queue = []
for line in queue_lines:
    parts = line.split('|', 5)
    if len(parts) != 6:
        continue
    job_id, name, state, elapsed, account, reason = parts
    job_id = job_id.strip()
    name = name.strip()
    state = state.strip()
    elapsed = elapsed.strip()
    account = account.strip()
    reason = reason.strip()
    queue.append({
        'job_id': job_id,
        'name': name,
        'state': state,
        'elapsed': elapsed,
        'account': account,
        'reason': reason,
        'tasks': array_task_count(job_id),
    })

expanded_queue_lines = subprocess.run(
    ['squeue', '-u', os.environ.get('USER', ''), '-h', '-r', '-o', '%.80j|%T|%a'],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    universal_newlines=True,
).stdout.splitlines()
expanded_account_counts = Counter()
expanded_state_counts = Counter()
expanded_hpo_jobs = []
for line in expanded_queue_lines:
    parts = line.split('|', 2)
    if len(parts) != 3:
        continue
    name, state, account = [part.strip() for part in parts]
    expanded_account_counts[(account, state)] += 1
    expanded_state_counts[state] += 1
    if '_recap_raw100_' in name:
        expanded_hpo_jobs.append({'name': name, 'state': state, 'account': account, 'tasks': 1})

print('ASHA/PIDSMaker summary')
print('Repo/storage path: {}'.format(root))
print('Legend: done=X/Y = valid metric JSONs; waiting_submit = backlog not submitted yet')
print('ckpt = valid runs with saved checkpoints; top_ckpt = checkpointed candidates needed for next additive rung.')
print('Free account slots are shared by all methods and are filled on controller polling loops.')
print('Queue: TOTAL={} RUNNING={} PENDING={} CONFIGURING={}'.format(
    sum(expanded_state_counts.values()),
    expanded_state_counts['RUNNING'],
    expanded_state_counts['PENDING'],
    expanded_state_counts['CONFIGURING'],
))
slot_parts = []
for account in ['p201223', 'p201219']:
    used = sum(expanded_account_counts[(account, state)] for state in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING'])
    free = max(0, 100 - used)
    slot_parts.append('{} used={}/100 free={}'.format(account, used, free))
print('Account slots: {}'.format('; '.join(slot_parts)))
gpu_hpo_jobs = [item for item in expanded_hpo_jobs if hpo_label(item['name'])[0]]
if gpu_hpo_jobs:
    print('GPU HPO detail:')
    grouped = Counter()
    for item in gpu_hpo_jobs:
        method, rung_token = hpo_label(item['name'])
        grouped[(method, rung_token, item['state'], item['account'])] += item['tasks']
    for state in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING']:
        state_rows = [(key, count) for key, count in grouped.items() if key[2] == state]
        for (method, rung_token, _, account), count in sorted(state_rows):
            print('  {} {} {} account={} tasks={}'.format(state, method, rung_token, account, count))
else:
    print('GPU HPO detail: none')
print('')

asha_root = root / 'meluxina' / 'pidsmaker' / 'asha_runs'
for method in methods:
    controller = [item for item in queue if item['name'] == controller_names[method]]
    gpu_jobs = [item for item in expanded_hpo_jobs if item['name'].startswith(method + '_recap_raw100_')]
    state_path = asha_root / '{}_recap_raw_100'.format(method) / 'state.json'

    print('== {} =='.format(method))
    if controller:
        item = controller[0]
        print('controller: {state} job={job_id} elapsed={elapsed}'.format(**item))
    else:
        print('controller: not in queue')

    if not state_path.exists():
        print('rungs: missing state file')
        print('')
        continue

    state_payload = json.loads(state_path.read_text())
    reduction_factor = int(state_payload.get('reduction_factor') or 2)
    printed_rung = False
    additive_blocked = None
    rung_names = ['r0_e1', 'r1_e2', 'r2_e4', 'r3_e9']
    for rung_index, rung in enumerate(rung_names):
        rung_state = state_payload.get('rungs', {}).get(rung, {})
        planned = len(rung_state.get('planned') or [])
        submitted = len(rung_state.get('submitted') or [])
        promoted = len(rung_state.get('promoted') or [])
        rung_token = rung.split('_', 1)[0]
        rung_gpu_jobs = [item for item in gpu_jobs if item['name'].startswith(method + '_recap_raw100_' + rung_token)]
        rung_gpu_counts = Counter()
        for item in rung_gpu_jobs:
            rung_gpu_counts[item['state']] += item['tasks']
        results_dir = asha_root / '{}_recap_raw_100'.format(method) / rung
        result_files = sorted(results_dir.glob('*.json')) if results_dir.exists() else []
        valid = 0
        checkpoints = 0
        failed = 0
        best_score = None
        valid_rows = []
        for path in result_files:
            try:
                row = json.loads(path.read_text())
            except Exception:
                continue
            score = row.get('adp_score')
            if row.get('phase') == 'hpo' and row.get('exit_code') == 0 and score is not None:
                valid += 1
                valid_rows.append(row)
                if row.get('checkpoint_saved') and row.get('checkpoint_path'):
                    checkpoints += 1
                score = float(score)
                if best_score is None or score > best_score:
                    best_score = score
            elif row.get('exit_code') not in (None, 0):
                failed += 1
        best = 'best_adp={:.3f}'.format(best_score) if best_score is not None else 'best_adp=-'
        if planned == 0 and submitted == 0 and promoted == 0 and not rung_gpu_jobs:
            continue
        checkpoint_need = max(1, planned // reduction_factor) if planned and rung_index < len(rung_names) - 1 else 0
        top_checkpoints = None
        if checkpoint_need and valid == planned:
            ranked_rows = sorted(valid_rows, key=lambda row: float(row.get('adp_score')), reverse=True)
            top_rows = ranked_rows[:checkpoint_need]
            top_checkpoints = sum(1 for row in top_rows if row.get('checkpoint_saved') and row.get('checkpoint_path'))
            if top_checkpoints < checkpoint_need and promoted == 0:
                additive_blocked = 'next additive rung needs top candidate checkpoints: top_ckpt={}/{}'.format(
                    top_checkpoints, checkpoint_need
                )
        printed_rung = True
        not_submitted = max(planned - submitted, 0) if planned else 0
        ckpt_text = 'ckpt={}'.format(checkpoints)
        if checkpoint_need:
            if top_checkpoints is None:
                ckpt_text += ' top_ckpt=pending/{}, after done={}/{}'.format(checkpoint_need, valid, planned)
            else:
                ckpt_text += ' top_ckpt={}/{}'.format(top_checkpoints, checkpoint_need)
        line = '{}: done={}/{} {} failed={} waiting_submit={} promoted={} {} slurm={}'.format(
            rung, valid, planned, ckpt_text, failed, not_submitted, promoted, best, slurm_counts_text(rung_gpu_counts)
        )
        print(line)
    if not printed_rung:
        print('rungs: none planned yet')
    if additive_blocked:
        print('blocked: {}'.format(additive_blocked))
    if controller and not gpu_jobs:
        print('note: controller is alive but no GPU task is currently queued/running for this method')
    print('')
PY"
}

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

progress() { r "if [ -f '$OUT' ]; then grep -E '\\[[0-9]+/[0-9]+\\]|ASHA status|account_capacity|submit rung|Submitted batch job|planned=|adp_score|MELUXINA_PIDSMAKER_ARTIFACT_ROOT|completed|wrote|Wrote|Error|error|Traceback|MemoryError' '$OUT' | tail -n '$LINES'; else printf 'Missing stdout log: %s\\n' '$OUT'; fi"; }
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

job_details() {
  while true; do
    pick_job
    while true; do
      printf '\nSelected %s (%s).\n' "$JOB_ID" "$JOB_NAME"
      PS3='Select view: '
      select view in Full Progress Logs Errors Memory Size 'Other job' 'Main menu' Quit; do
        case ${view:-} in
          Full) s Queue; r "squeue -j '$JOB_ID' -o '%.18i %.9P %.24j %.2t %.12M %.12L %.6D %R'"; s Progress; progress; s Errors; errors; s Size; size; s Memory; memory; break ;;
          Progress) s Progress; progress; break ;;
          Logs) s Logs; logs; break ;;
          Errors) s Errors; errors; break ;;
          Memory) s Memory; memory; break ;;
          Size) s Size; size; break ;;
          'Other job') break 2 ;;
          'Main menu') return ;;
          Quit) exit 0 ;;
          *) printf 'Invalid selection.\n' ;;
        esac
      done
    done
  done
}

if [[ ${1:-} == summary || ${1:-} == asha ]]; then
  asha_summary
  exit 0
fi

while true; do
  printf '\nMeluXina Job Status\n'
  PS3='Select action: '
  select action in Summary 'Job details' Quit; do
    case ${action:-} in
      Summary) s Summary; asha_summary; break ;;
      'Job details') job_details; break ;;
      Quit) exit 0 ;;
      *) printf 'Invalid selection.\n' ;;
    esac
  done
done
