#!/usr/bin/env bash
set -euo pipefail

REPO=${MELUXINA_REPO:-/mnt/tier2/project/p201223/pidsmaker-across-capture-tools}
SSH_HOST=${MELUXINA_SSH_HOST:-u101059@login.lxp.lu}
SSH_KEY=${MELUXINA_SSH_KEY:-/home/omar/.ssh/meluxina}
SSH_PORT=${MELUXINA_SSH_PORT:-8822}
SSH_CONNECT_TIMEOUT=${MELUXINA_SSH_CONNECT_TIMEOUT:-15}
LINES=${MELUXINA_STATUS_LINES:-40}
HPO_TOP=${MELUXINA_HPO_TOP:-5}

r() { ssh -n -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" -i "$SSH_KEY" -p "$SSH_PORT" "$SSH_HOST" "$1"; }
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

hpo_summary() {
  r "REPO='${REPO%/}' HPO_TOP='${HPO_TOP}' python3 - <<'PY'
import json
import os
import re
import subprocess
from collections import Counter
from pathlib import Path

root = Path(os.environ['REPO'])
asha_root = root / 'meluxina' / 'pidsmaker' / 'asha_runs'
methods = ['velox', 'magic', 'orthrus', 'kairos']
rung_names = ['r0_e1', 'r1_e2', 'r2_e4', 'r3_e9']
top_n = int(os.environ.get('HPO_TOP') or 5)

def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None

def fmt(value, digits=3):
    value = safe_float(value)
    if value is None:
        return '-'
    return ('{:.%df}' % digits).format(value).rstrip('0').rstrip('.')

def compact_value(value):
    if isinstance(value, float):
        return '{:g}'.format(value)
    return str(value)

def checkpoint_ok(row):
    path = row.get('persist_checkpoint') or row.get('checkpoint_path')
    return bool(row.get('checkpoint_saved') and path and Path(path).exists())

def load_result_rows(method, rung, phase='hpo'):
    results_dir = asha_root / '{}_recap_raw_100'.format(method) / rung
    rows = []
    failed = []
    if not results_dir.exists():
        return rows, failed
    for path in sorted(results_dir.glob('*.json')):
        try:
            row = json.loads(path.read_text())
        except Exception:
            continue
        score = safe_float(row.get('adp_score'))
        if row.get('phase') == phase and row.get('exit_code') == 0 and score is not None:
            row['_score'] = score
            rows.append(row)
        elif row.get('phase') == phase and row.get('exit_code') not in (None, 0):
            failed.append(row)
    rows.sort(key=lambda item: item.get('_score', float('-inf')), reverse=True)
    return rows, failed

def failure_text(row):
    name = row.get('name') or '?'
    exit_code = row.get('exit_code')
    oom = row.get('oom')
    job = row.get('slurm_job_id') or '-'
    task = row.get('slurm_array_task_id') or '-'
    return '{}(exit={} oom={} job={}_{})'.format(name, exit_code, oom, job, task)

def slurm_counts_text(counter):
    parts = []
    for state in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING']:
        count = counter[state]
        if count:
            parts.append('{}={}'.format(state, count))
    return ' '.join(parts) if parts else 'none'

def short_window(row):
    variant = row.get('export_variant')
    if variant:
        return variant.replace('window_', '')
    seconds = row.get('export_window_size_seconds')
    if seconds:
        try:
            seconds = int(seconds)
            if seconds % 60 == 0:
                return '{}m'.format(seconds // 60)
            return '{}s'.format(seconds)
        except Exception:
            return str(seconds)
    return None

def config_bits(row):
    overrides = row.get('overrides') or {}
    bits = []
    window = short_window(row)
    if window:
        bits.append('win={}'.format(window))

    def add(label, *keys):
        for key in keys:
            if key in overrides:
                bits.append('{}={}'.format(label, compact_value(overrides[key])))
                return

    add('lr', 'training.lr')
    add('wd', 'training.weight_decay')
    add('hid', 'training.node_hid_dim')
    add('out', 'training.node_out_dim')
    add('emb', 'featurization.emb_dim')
    add('mem', 'training.encoder.tgn.tgn_memory_dim')
    add('time', 'training.encoder.tgn.tgn_time_dim')
    add('batch', 'batching.intra_graph_batching.edges.intra_graph_batch_size')
    add('neigh', 'batching.intra_graph_batching.tgn_last_neighbor.tgn_neighbor_size')
    add('mask', 'training.decoder.reconstruct_masked_features.mask_rate')

    name = row.get('name') or ''
    match = re.search(r'_cap([^_]+)_', name)
    if match:
        bits.append('cap={}'.format(match.group(1)))
    return ' '.join(bits) if bits else '-'

def metrics_text(row):
    parts = [
        'adp={}'.format(fmt(row.get('adp_score'), 3)),
        'auc={}'.format(fmt(row.get('auc'), 3)),
        'ap={}'.format(fmt(row.get('ap'), 3)),
        'rec={}'.format(fmt(row.get('recall'), 4)),
        'prec={}'.format(fmt(row.get('precision'), 4)),
        'atk={}'.format(fmt(row.get('percent_detected_attacks'), 3)),
    ]
    if row.get('best_val_loss') is not None:
        parts.append('val_loss={}'.format(fmt(row.get('best_val_loss'), 4)))
    if row.get('duration_sec') is not None:
        parts.append('dur={}m'.format(fmt(safe_float(row.get('duration_sec')) / 60.0, 1)))
    return ' '.join(parts)

def print_leaderboard(rows, source_rung):
    if not rows:
        print('leaderboard: no completed results yet')
        return
    print('leaderboard: source={} top={}'.format(source_rung, min(top_n, len(rows))))
    for rank, row in enumerate(rows[:top_n], start=1):
        ckpt = 'yes' if checkpoint_ok(row) else 'no'
        print('  #{:<2} {} ckpt={} {}'.format(rank, metrics_text(row), ckpt, row.get('name')))
        print('      {}'.format(config_bits(row)))

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

controllers = {}
for line in queue_lines:
    parts = line.split('|', 5)
    if len(parts) != 6:
        continue
    job_id, name, state, elapsed, account, reason = [part.strip() for part in parts]
    for method in methods:
        if name == 'pids_asha_' + method:
            controllers[method] = {
                'job_id': job_id,
                'state': state,
                'elapsed': elapsed,
                'account': account,
                'reason': reason,
            }

expanded_queue_lines = subprocess.run(
    ['squeue', '-u', os.environ.get('USER', ''), '-h', '-r', '-o', '%.80j|%T|%a'],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    universal_newlines=True,
).stdout.splitlines()
slurm_counts = Counter()
for line in expanded_queue_lines:
    parts = line.split('|', 2)
    if len(parts) != 3:
        continue
    name, state, account = [part.strip() for part in parts]
    method, rung_token = hpo_label(name)
    if method and rung_token:
        slurm_counts[(method, rung_token, state)] += 1

print('PIDSMaker ASHA HPO detail')
print('Repo/storage path: {}'.format(root))
print('Metric: adp_score, maximize. Leaderboard uses the deepest rung with completed results.')
print('Use MELUXINA_HPO_TOP=N to change the number of configs shown. Current top={}.'.format(top_n))

for method in methods:
    run_root = asha_root / '{}_recap_raw_100'.format(method)
    state_path = run_root / 'state.json'
    print('')
    print('== {} =='.format(method))
    controller = controllers.get(method)
    if controller:
        print('controller: {state} job={job_id} elapsed={elapsed} account={account}'.format(**controller))
    else:
        print('controller: not in queue')
    if not state_path.exists():
        print('state: missing {}'.format(state_path))
        continue

    state_payload = json.loads(state_path.read_text())
    reduction_factor = int(state_payload.get('reduction_factor') or 2)
    print('state: updated={} policy={} reduction_factor={}'.format(
        state_payload.get('updated_at', '-'),
        state_payload.get('promotion_policy', '-'),
        reduction_factor,
    ))

    all_rows = {}
    best_source_rung = None
    best_source_rows = []
    print('rungs:')
    for index, rung in enumerate(rung_names):
        rung_state = state_payload.get('rungs', {}).get(rung, {})
        planned_names = rung_state.get('planned') or []
        submitted_names = rung_state.get('submitted') or []
        promoted_names = rung_state.get('promoted') or []
        cancelled_names = rung_state.get('cancelled') or []
        planned = len(planned_names)
        submitted = len(submitted_names)
        promoted = len(promoted_names)
        cancelled = len(cancelled_names)
        rows, failed_rows = load_result_rows(method, rung)
        all_rows[rung] = rows
        if rows:
            best_source_rung = rung
            best_source_rows = rows
        if planned == 0 and submitted == 0 and promoted == 0 and not rows:
            continue
        ckpts = sum(1 for row in rows if checkpoint_ok(row))
        best = fmt(rows[0].get('adp_score'), 3) if rows else '-'
        target = 0
        cutoff = '-'
        target_text = '-'
        if index < len(rung_names) - 1 and planned:
            target = max(1, planned // reduction_factor)
            target_text = str(target)
            if len(rows) >= planned and len(rows) >= target:
                cutoff = fmt(rows[target - 1].get('adp_score'), 3)
        rung_token = rung.split('_', 1)[0]
        rung_slurm_counts = Counter()
        for state_name in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING']:
            rung_slurm_counts[state_name] = slurm_counts[(method, rung_token, state_name)]
        print('  {}: done={}/{} submitted={} ckpt={} failed={} cancelled={} promoted={}/{} best_adp={} cutoff={} slurm={}'.format(
            rung,
            len(rows),
            planned,
            submitted,
            ckpts,
            len(failed_rows),
            cancelled,
            promoted,
            target_text,
            best,
            cutoff,
            slurm_counts_text(rung_slurm_counts),
        ))
        if failed_rows:
            samples = [failure_text(row) for row in failed_rows[:3]]
            suffix = '' if len(failed_rows) <= 3 else ' (+{} more)'.format(len(failed_rows) - 3)
            print('    failures: {}{}'.format('; '.join(samples), suffix))

    print_leaderboard(best_source_rows, best_source_rung or '-')

    print('promotion:')
    printed_promotion = False
    for index, rung in enumerate(rung_names[:-1]):
        next_rung = rung_names[index + 1]
        rung_state = state_payload.get('rungs', {}).get(rung, {})
        next_state = state_payload.get('rungs', {}).get(next_rung, {})
        planned_names = rung_state.get('planned') or []
        promoted_names = rung_state.get('promoted') or []
        rows = all_rows.get(rung, [])
        if not planned_names and not promoted_names and not next_state.get('planned'):
            continue
        printed_promotion = True
        target = max(1, len(planned_names) // reduction_factor) if planned_names else len(promoted_names)
        if len(rows) < len(planned_names):
            remaining = len(planned_names) - len(rows)
            print('  {} -> {}: waiting for {} more results; target={}'.format(rung, next_rung, remaining, target))
            continue
        ranked_names = [row.get('name') for row in rows[:target]]
        promoted_set = set(promoted_names)
        ranked_set = set(ranked_names)
        missing_from_promotion = sorted(ranked_set - promoted_set)
        unexpected_promotions = sorted(promoted_set - ranked_set)
        rows_by_name = {row.get('name'): row for row in rows}
        missing_ckpt = [name for name in promoted_names if not checkpoint_ok(rows_by_name.get(name, {}))]
        next_planned_set = set(next_state.get('planned') or [])
        planned_mismatch = sorted(promoted_set - next_planned_set) if next_planned_set else []
        if promoted_names and not missing_from_promotion and not unexpected_promotions and not missing_ckpt and not planned_mismatch:
            cutoff = fmt(rows[target - 1].get('adp_score'), 3) if len(rows) >= target else '-'
            print('  {} -> {}: OK top{} selected; cutoff={}; submitted_next={}/{}; missing_ckpt=0'.format(
                rung,
                next_rung,
                target,
                cutoff,
                len(next_state.get('submitted') or []),
                len(next_state.get('planned') or []),
            ))
        elif not promoted_names:
            print('  {} -> {}: ready to promote top{} on next controller pass'.format(rung, next_rung, target))
        else:
            print('  {} -> {}: CHECK missing_top={} unexpected={} missing_ckpt={} next_plan_mismatch={}'.format(
                rung,
                next_rung,
                len(missing_from_promotion),
                len(unexpected_promotions),
                len(missing_ckpt),
                len(planned_mismatch),
            ))
    if not printed_promotion:
        print('  no active promotion decisions yet')

    final_state = state_payload.get('final') or {}
    final_rows, final_failed_rows = load_result_rows(method, 'final_e12', phase='final')
    final_planned = final_state.get('planned') or []
    final_submitted = final_state.get('submitted') or []
    if final_planned or final_submitted or final_rows or final_failed_rows:
        final_source = final_state.get('source_rung') or 'r3_e9'
        final_top_k = final_state.get('top_k') or 3
        final_slurm_counts = Counter()
        for state_name in ['RUNNING', 'PENDING', 'CONFIGURING', 'COMPLETING']:
            final_slurm_counts[state_name] = slurm_counts[(method, 'final', state_name)]
        print('final: source={} top_k={} done={}/{} submitted={} ckpt={} failed={} best_adp={} slurm={}'.format(
            final_source,
            final_top_k,
            len(final_rows),
            len(final_planned),
            len(final_submitted),
            sum(1 for row in final_rows if checkpoint_ok(row)),
            len(final_failed_rows),
            fmt(final_rows[0].get('adp_score'), 3) if final_rows else '-',
            slurm_counts_text(final_slurm_counts),
        ))
        if final_planned:
            print('  planned: {}'.format(', '.join(final_planned)))
        if final_failed_rows:
            samples = [failure_text(row) for row in final_failed_rows[:3]]
            suffix = '' if len(final_failed_rows) <= 3 else ' (+{} more)'.format(len(final_failed_rows) - 3)
            print('  failures: {}{}'.format('; '.join(samples), suffix))
        if final_rows:
            print_leaderboard(final_rows, 'final_e12')
PY"
}

final_results() {
  r "REPO='${REPO%/}' python3 - <<'PY'
import json
import os
import re
from pathlib import Path

root = Path(os.environ['REPO'])
asha_root = root / 'meluxina' / 'pidsmaker' / 'asha_runs'
methods = ['velox', 'magic', 'orthrus', 'kairos']
known_override_keys = {
    'batching.intra_graph_batching.edges.intra_graph_batch_size',
    'batching.intra_graph_batching.tgn_last_neighbor.tgn_neighbor_size',
    'featurization.emb_dim',
    'featurization.seed',
    'training.decoder.reconstruct_masked_features.mask_rate',
    'training.encoder.tgn.tgn_memory_dim',
    'training.encoder.tgn.tgn_time_dim',
    'training.lr',
    'training.node_hid_dim',
    'training.node_out_dim',
    'training.seed',
    'training.weight_decay',
}

def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None

def fmt(value, digits=3):
    value = safe_float(value)
    if value is None:
        return '-'
    return ('{:.%df}' % digits).format(value).rstrip('0').rstrip('.')

def fmt_int(value):
    if value is None:
        return '-'
    try:
        return str(int(value))
    except Exception:
        return str(value)

def compact_value(value):
    if value is None:
        return '-'
    if isinstance(value, float):
        return '{:g}'.format(value)
    return str(value)

def override_value(overrides, key):
    if key in overrides:
        return compact_value(overrides.get(key))
    return '-'

def short_window(row):
    variant = row.get('export_variant')
    if variant:
        return variant.replace('window_', '')
    seconds = row.get('export_window_size_seconds')
    if seconds:
        try:
            seconds = int(seconds)
            if seconds % 60 == 0:
                return '{}m'.format(seconds // 60)
            return '{}s'.format(seconds)
        except Exception:
            return str(seconds)
    return '-'

def cap_value(row):
    match = re.search(r'_cap([^_]+)_', str(row.get('name') or ''))
    return match.group(1) if match else '-'

def checkpoint_ok(row):
    path = row.get('persist_checkpoint') or row.get('checkpoint_path')
    return bool(row.get('checkpoint_saved') and path and Path(str(path)).exists())

def load_rows(method, rung, phase):
    results_dir = asha_root / '{}_recap_raw_100'.format(method) / rung
    rows = []
    if not results_dir.exists():
        return rows
    for path in sorted(results_dir.glob('*.json')):
        try:
            row = json.loads(path.read_text())
        except Exception:
            continue
        if row.get('phase') == phase and row.get('exit_code') == 0 and safe_float(row.get('adp_score')) is not None:
            rows.append(row)
    rows.sort(key=lambda item: safe_float(item.get('adp_score')) or float('-inf'), reverse=True)
    return rows

def extra_overrides(overrides):
    extras = []
    for key in sorted(overrides):
        if key not in known_override_keys:
            extras.append('{}={}'.format(key, compact_value(overrides.get(key))))
    return ';'.join(extras) if extras else '-'

def table(rows, columns):
    if not rows:
        print('No final result rows found.')
        return
    widths = []
    for key, header in columns:
        width = len(header)
        for row in rows:
            width = max(width, len(str(row.get(key, '-'))))
        widths.append(width)
    print(' | '.join(header.ljust(widths[index]) for index, (_, header) in enumerate(columns)))
    print('-+-'.join('-' * width for width in widths))
    for row in rows:
        print(' | '.join(str(row.get(key, '-')).ljust(widths[index]) for index, (key, _) in enumerate(columns)))

print('PIDSMaker final e12 results')
print('Repo/storage path: {}'.format(root))
print('Rows are final fresh 12-epoch runs. source_rank/source_adp refer to the r3_e9 HPO result that selected the final run.')
print('')

rows = []
missing = []
for method in methods:
    run_root = asha_root / '{}_recap_raw_100'.format(method)
    state_path = run_root / 'state.json'
    final_planned = []
    if state_path.exists():
        try:
            final_planned = (json.loads(state_path.read_text()).get('final') or {}).get('planned') or []
        except Exception:
            final_planned = []

    source_rows = load_rows(method, 'r3_e9', 'hpo')
    source_by_name = {str(row.get('name')): (rank, row) for rank, row in enumerate(source_rows, start=1)}
    final_rows = load_rows(method, 'final_e12', 'final')
    final_by_name = {str(row.get('name')): row for row in final_rows}

    for rank, row in enumerate(final_rows, start=1):
        name = str(row.get('name') or '-')
        overrides = row.get('overrides') or {}
        source_rank, source_row = source_by_name.get(name, ('-', {}))
        selected_rank = final_planned.index(name) + 1 if name in final_planned else '-'
        duration_sec = safe_float(row.get('duration_sec'))
        rows.append({
            'method': method,
            'final_rank': rank,
            'selected_rank': selected_rank,
            'source_rank': source_rank,
            'config': name,
            'adp': fmt(row.get('adp_score'), 3),
            'auc': fmt(row.get('auc'), 3),
            'ap': fmt(row.get('ap'), 3),
            'recall': fmt(row.get('recall'), 4),
            'precision': fmt(row.get('precision'), 4),
            'attack_pct': fmt(row.get('percent_detected_attacks'), 3),
            'disc': fmt(row.get('discrimination'), 3),
            'tp': fmt_int(row.get('tp')),
            'fp': fmt_int(row.get('fp')),
            'tn': fmt_int(row.get('tn')),
            'fn': fmt_int(row.get('fn')),
            'best_val_loss': fmt(row.get('best_val_loss'), 4),
            'test_loss': fmt(row.get('test_loss'), 4),
            'best_epoch': fmt_int(row.get('best_epoch')),
            'test_epoch': fmt_int(row.get('test_epoch')),
            'duration_min': fmt(duration_sec / 60.0 if duration_sec is not None else None, 1),
            'epochs': fmt_int(row.get('epochs')),
            'source_adp': fmt(source_row.get('adp_score') if isinstance(source_row, dict) else None, 3),
            'ckpt': 'yes' if checkpoint_ok(row) else 'no',
            'window': short_window(row),
            'lr': override_value(overrides, 'training.lr'),
            'weight_decay': override_value(overrides, 'training.weight_decay'),
            'hid': override_value(overrides, 'training.node_hid_dim'),
            'out': override_value(overrides, 'training.node_out_dim'),
            'emb': override_value(overrides, 'featurization.emb_dim'),
            'mem': override_value(overrides, 'training.encoder.tgn.tgn_memory_dim'),
            'time': override_value(overrides, 'training.encoder.tgn.tgn_time_dim'),
            'batch': override_value(overrides, 'batching.intra_graph_batching.edges.intra_graph_batch_size'),
            'neigh': override_value(overrides, 'batching.intra_graph_batching.tgn_last_neighbor.tgn_neighbor_size'),
            'mask': override_value(overrides, 'training.decoder.reconstruct_masked_features.mask_rate'),
            'cap': cap_value(row),
            'train_seed': override_value(overrides, 'training.seed'),
            'feat_seed': override_value(overrides, 'featurization.seed'),
            'extra_overrides': extra_overrides(overrides),
        })

    for name in final_planned:
        if name not in final_by_name:
            missing.append('{}:{}'.format(method, name))

columns = [
    ('method', 'method'),
    ('final_rank', 'final_rank'),
    ('selected_rank', 'selected_rank'),
    ('source_rank', 'source_rank'),
    ('config', 'config'),
    ('adp', 'adp'),
    ('auc', 'auc'),
    ('ap', 'ap'),
    ('recall', 'recall'),
    ('precision', 'precision'),
    ('attack_pct', 'attack_pct'),
    ('disc', 'disc'),
    ('tp', 'tp'),
    ('fp', 'fp'),
    ('tn', 'tn'),
    ('fn', 'fn'),
    ('best_val_loss', 'best_val_loss'),
    ('test_loss', 'test_loss'),
    ('best_epoch', 'best_epoch'),
    ('test_epoch', 'test_epoch'),
    ('duration_min', 'duration_min'),
    ('epochs', 'epochs'),
    ('source_adp', 'source_adp'),
    ('ckpt', 'ckpt'),
    ('window', 'window'),
    ('lr', 'lr'),
    ('weight_decay', 'weight_decay'),
    ('hid', 'hid'),
    ('out', 'out'),
    ('emb', 'emb'),
    ('mem', 'mem'),
    ('time', 'time'),
    ('batch', 'batch'),
    ('neigh', 'neigh'),
    ('mask', 'mask'),
    ('cap', 'cap'),
    ('train_seed', 'train_seed'),
    ('feat_seed', 'feat_seed'),
    ('extra_overrides', 'extra_overrides'),
]
table(rows, columns)
if missing:
    print('')
    print('Missing planned final results: {}'.format(', '.join(missing)))
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
if [[ ${1:-} == hpo ]]; then
  hpo_summary
  exit 0
fi
if [[ ${1:-} == final || ${1:-} == finals || ${1:-} == final-results ]]; then
  final_results
  exit 0
fi

while true; do
  printf '\nMeluXina Job Status\n'
  PS3='Select action: '
  select action in Summary 'HPO detail' 'Final results' 'Job details' Quit; do
    case ${action:-} in
      Summary) s Summary; asha_summary; break ;;
      'HPO detail') s 'HPO detail'; hpo_summary; break ;;
      'Final results') s 'Final results'; final_results; break ;;
      'Job details') job_details; break ;;
      Quit) exit 0 ;;
      *) printf 'Invalid selection.\n' ;;
    esac
  done
done
