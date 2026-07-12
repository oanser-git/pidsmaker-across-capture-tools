#!/usr/bin/env python3
"""Interactive MeluXina/PIDSMaker status helper."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from collections import Counter
from typing import Any


DEFAULT_REPO = "/mnt/tier2/project/p201223/pidsmaker-across-capture-tools"
METHODS = ["magic", "velox", "orthrus", "kairos"]
FINAL_METHODS = ["velox", "magic", "orthrus", "kairos"]
METRIC = "adp_score"


def env(name, default):
    return os.environ.get(name, default)


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt(value, digits=3):
    number = safe_float(value)
    if number is None:
        return "-"
    return f"{number:.{digits}f}"


def run_namespace(method, suffix):
    return f"{method}_recap_raw_100{('_' + suffix) if suffix else ''}"


def job_prefix(method, suffix):
    return f"{method}_recap_raw100{('_' + suffix) if suffix else ''}"


def state_rungs(payload):
    def key(name):
        match = re.match(r"r(\d+)_", name)
        return (int(match.group(1)) if match else 999, name)

    return sorted((payload.get("rungs") or {}).keys(), key=key)


def array_task_count(job_id):
    if "_[" not in job_id:
        return 1
    try:
        inside = job_id.split("_[", 1)[1].rsplit("]", 1)[0].split("%", 1)[0]
        total = 0
        for segment in inside.split(","):
            if not segment:
                continue
            if "-" in segment:
                start, end = segment.split("-", 1)
                total += int(end) - int(start) + 1
            else:
                total += 1
        return max(total, 1)
    except Exception:
        return 1


def run_cmd(command, check=False, input_text=None):
    return subprocess.run(
        command,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        check=check,
    )


def remote_ssh_args(args, remote_command):
    ssh_args = [
        "ssh",
        "-n",
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={args.ssh_connect_timeout}",
        "-i",
        args.ssh_key,
        "-p",
        str(args.ssh_port),
        args.ssh_host,
        " ".join(shlex.quote(part) for part in remote_command),
    ]
    return ssh_args


def run_remote(args, action, capture=False, **extra):
    remote_script = f"{args.repo.rstrip('/')}/meluxina/job_status.py"
    command = [
        "python3",
        remote_script,
        "--remote",
        "--repo",
        args.repo,
        "--run-suffix",
        args.run_suffix,
        "--lines",
        str(args.lines),
        "--hpo-top",
        str(args.hpo_top),
        action,
    ]
    for key, value in extra.items():
        if value is not None:
            command.extend([f"--{key.replace('_', '-')}", str(value)])
    proc = run_cmd(remote_ssh_args(args, command))
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        raise SystemExit(proc.returncode)
    if capture:
        return proc.stdout
    print(proc.stdout, end="")
    return ""


def asha_root(repo):
    return repo / "meluxina" / "pidsmaker" / "asha_runs"


def read_json(path):
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_rows(repo, method, suffix, rung, phase):
    directory = asha_root(repo) / run_namespace(method, suffix) / rung
    rows = []
    failed = []
    if not directory.exists():
        return rows, failed
    for path in sorted(directory.glob("*.json")):
        row = read_json(path)
        if row.get("phase", phase) != phase:
            continue
        if int(row.get("exit_code", 1)) == 0 and safe_float(row.get(METRIC)) is not None:
            rows.append(row)
        else:
            failed.append(path)
    rows.sort(key=lambda row: safe_float(row.get(METRIC)) or float("-inf"), reverse=True)
    return rows, failed


def checkpoint_ok(row):
    path = row.get("checkpoint_path")
    return bool(row.get("checkpoint_saved") and path and Path(str(path)).exists())


def table(rows, columns):
    widths = []
    for key, label in columns:
        widths.append(max(len(label), *(len(str(row.get(key, "-"))) for row in rows)))
    print("  ".join(label.ljust(width) for (_, label), width in zip(columns, widths)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(str(row.get(key, "-")).ljust(width) for (key, _), width in zip(columns, widths)))


def queue_rows():
    proc = run_cmd(["squeue", "-u", env("USER", ""), "-h", "-o", "%.120i|%.80j|%T|%M|%a|%R"])
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.split("|", 5)
        if len(parts) != 6:
            continue
        job_id, name, state, elapsed, account, reason = [part.strip() for part in parts]
        rows.append(
            {
                "job_id": job_id,
                "name": name,
                "state": state,
                "elapsed": elapsed,
                "account": account,
                "reason": reason,
                "tasks": array_task_count(job_id),
            }
        )
    return rows


def expanded_queue_rows():
    proc = run_cmd(["squeue", "-u", env("USER", ""), "-h", "-r", "-o", "%.80j|%T|%a"])
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        name, state, account = [part.strip() for part in parts]
        rows.append({"name": name, "state": state, "account": account, "tasks": 1})
    return rows


def hpo_label(name, suffix):
    for method in METHODS:
        prefix = job_prefix(method, suffix) + "_"
        if name.startswith(prefix):
            return method, name[len(prefix) :].split("_", 1)[0]
    return None, None


def controller_for(method, suffix, queue):
    names = {"pids_asha_" + method, "pids_asha_" + method + (("_" + suffix) if suffix else "")}
    for item in queue:
        if item["name"] in names or item["name"].startswith("asha_" + method):
            return item
    return None


def slurm_counts_text(counter):
    parts = [f"{state}={counter[state]}" for state in ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"] if counter[state]]
    return " ".join(parts) if parts else "none"


def remote_list_runs(args):
    root = Path(args.repo)
    runs = {}
    for path in asha_root(root).glob("*_recap_raw_100*"):
        if not path.is_dir():
            continue
        for method in METHODS:
            prefix = f"{method}_recap_raw_100"
            if path.name == prefix:
                suffix = ""
            elif path.name.startswith(prefix + "_"):
                suffix = path.name[len(prefix) + 1 :]
            else:
                continue
            info = runs.setdefault(suffix, {"suffix": suffix, "methods": [], "state_count": 0, "rungs": set(), "final_jsons": 0, "latest_mtime": 0.0, "updated": "-"})
            info["methods"].append(method)
            state_path = path / "state.json"
            if state_path.exists():
                info["state_count"] += 1
                info["latest_mtime"] = max(info["latest_mtime"], state_path.stat().st_mtime)
                state = read_json(state_path)
                info["rungs"].update(state_rungs(state))
                info["updated"] = max(str(info["updated"]), str(state.get("updated_at") or "-"))
            final_dir = path / "final_e12"
            if final_dir.exists():
                info["final_jsons"] += len(list(final_dir.glob("*.json")))
    output = []
    for info in runs.values():
        suffix = str(info["suffix"])
        display = "original/default model-weight resume run" if not suffix else suffix
        methods = sorted(info["methods"])
        output.append(
            {
                "suffix": suffix,
                "display": display,
                "details": "methods={}/{} state={}/{} rungs={} final_jsons={} updated={}".format(
                    len(methods),
                    len(METHODS),
                    info["state_count"],
                    len(METHODS),
                    ",".join(sorted(info["rungs"])) if info["rungs"] else "-",
                    info["final_jsons"],
                    info["updated"],
                ),
                "latest_mtime": info["latest_mtime"],
            }
        )
    output.sort(key=lambda item: (item["latest_mtime"], bool(item["suffix"]), item["suffix"]), reverse=True)
    print(json.dumps(output))


def remote_summary(args):
    repo = Path(args.repo)
    suffix = args.run_suffix.strip("_")
    queue = queue_rows()
    expanded = expanded_queue_rows()
    account_counts = Counter((row["account"], row["state"]) for row in expanded)
    state_counts = Counter(row["state"] for row in expanded)
    hpo_jobs = [row for row in expanded if hpo_label(row["name"], suffix)[0]]

    print("ASHA/PIDSMaker summary")
    print(f"Repo/storage path: {repo}")
    if suffix:
        print(f"Run suffix: {suffix}")
    print("Legend: done=X/Y = valid metric JSONs; waiting_submit = backlog not submitted yet")
    print("ckpt = valid runs with saved checkpoints; top_ckpt = checkpointed candidates needed for next additive rung.")
    print("Free account slots are shared by all methods and are filled on controller polling loops.")
    print("Queue: TOTAL={} RUNNING={} PENDING={} CONFIGURING={}".format(sum(state_counts.values()), state_counts["RUNNING"], state_counts["PENDING"], state_counts["CONFIGURING"]))
    slot_text = []
    for account in ["p201223", "p201219"]:
        used = sum(account_counts[(account, state)] for state in ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"])
        slot_text.append(f"{account} used={used}/100 free={max(0, 100 - used)}")
    print("Account slots: " + "; ".join(slot_text))

    if hpo_jobs:
        print("GPU HPO detail:")
        grouped = Counter()
        for row in hpo_jobs:
            method, rung_token = hpo_label(row["name"], suffix)
            grouped[(method, rung_token, row["state"], row["account"])] += row["tasks"]
        for state in ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"]:
            for (method, rung_token, _, account), count in sorted((key, value) for key, value in grouped.items() if key[2] == state):
                print(f"  {state} {method} {rung_token} account={account} tasks={count}")
    else:
        print("GPU HPO detail: none")
    print("")

    for method in METHODS:
        run_root = asha_root(repo) / run_namespace(method, suffix)
        state_path = run_root / "state.json"
        controller = controller_for(method, suffix, queue)
        gpu_jobs = [row for row in hpo_jobs if row["name"].startswith(job_prefix(method, suffix) + "_")]

        print(f"== {method} ==")
        if controller:
            print("controller: {state} job={job_id} elapsed={elapsed}".format(**controller))
        else:
            print("controller: not in queue")
        if not state_path.exists():
            print("rungs: missing state file\n")
            continue

        state = read_json(state_path)
        reduction = int(state.get("reduction_factor") or 2)
        for index, rung in enumerate(state_rungs(state)):
            rung_state = (state.get("rungs") or {}).get(rung, {})
            planned = len(rung_state.get("planned") or [])
            submitted = len(rung_state.get("submitted") or [])
            promoted = len(rung_state.get("promoted") or [])
            rows, failed = load_rows(repo, method, suffix, rung, "hpo")
            ckpt = sum(1 for row in rows if checkpoint_ok(row))
            top_k = planned // reduction if index < len(state_rungs(state)) - 1 else len(rows)
            top_ckpt = sum(1 for row in rows[:top_k] if checkpoint_ok(row)) if top_k else 0
            best = fmt(rows[0].get(METRIC), 3) if rows else "-"
            rung_token = rung.split("_", 1)[0]
            rung_counts = Counter(row["state"] for row in gpu_jobs if row["name"].startswith(job_prefix(method, suffix) + "_" + rung_token))
            if planned == 0 and submitted == 0 and not rows and not failed and not rung_counts:
                continue
            top_text = f"{top_ckpt}/{top_k}" if len(rows) >= planned and top_k else f"pending/{top_k}" if top_k else "-"
            print(
                f"{rung}: done={len(rows)}/{planned} ckpt={ckpt} top_ckpt={top_text}, "
                f"after done={len(rows)}/{planned} failed={len(failed)} waiting_submit={max(0, planned - submitted)} "
                f"promoted={promoted} best_adp={best} slurm={slurm_counts_text(rung_counts)}"
            )
        if controller and not gpu_jobs:
            print("note: controller is alive but no GPU task is currently queued/running for this method")
        print("")


def remote_hpo(args):
    repo = Path(args.repo)
    suffix = args.run_suffix.strip("_")
    queue = queue_rows()
    expanded = expanded_queue_rows()
    slurm_counts = Counter()
    for row in expanded:
        method, rung_token = hpo_label(row["name"], suffix)
        if method and rung_token:
            slurm_counts[(method, rung_token, row["state"])] += 1

    print("PIDSMaker ASHA HPO detail")
    print(f"Repo/storage path: {repo}")
    if suffix:
        print(f"Run suffix: {suffix}")
    print(f"Metric: {METRIC}, maximize. Leaderboard uses the deepest rung with completed results.")
    print(f"Use MELUXINA_HPO_TOP=N to change the number of configs shown. Current top={args.hpo_top}.")

    for method in FINAL_METHODS:
        run_root = asha_root(repo) / run_namespace(method, suffix)
        state_path = run_root / "state.json"
        print(f"\n== {method} ==")
        controller = controller_for(method, suffix, queue)
        if controller:
            print("controller: {state} job={job_id} elapsed={elapsed} account={account}".format(**controller))
        else:
            print("controller: not in queue")
        if not state_path.exists():
            print(f"state: missing {state_path}")
            continue

        state = read_json(state_path)
        print("state: updated={} policy={} reduction_factor={}".format(state.get("updated_at", "-"), state.get("promotion_policy", "-"), state.get("reduction_factor", "-")))
        best_rung = None
        best_rows = []
        print("rungs:")
        for rung in state_rungs(state):
            rung_state = (state.get("rungs") or {}).get(rung, {})
            planned = len(rung_state.get("planned") or [])
            submitted = len(rung_state.get("submitted") or [])
            promoted = len(rung_state.get("promoted") or [])
            rows, failed = load_rows(repo, method, suffix, rung, "hpo")
            ckpt = sum(1 for row in rows if checkpoint_ok(row))
            if rows:
                best_rung = rung
                best_rows = rows
            rung_token = rung.split("_", 1)[0]
            counts = Counter({state_name: slurm_counts[(method, rung_token, state_name)] for state_name in ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"]})
            best = fmt(rows[0].get(METRIC), 3) if rows else "-"
            print(
                f"  {rung}: planned={planned} submitted={submitted} done={len(rows)} failed={len(failed)} "
                f"ckpt={ckpt} promoted={promoted} best_adp={best} slurm={slurm_counts_text(counts)}"
            )
        if best_rows:
            print(f"top {min(args.hpo_top, len(best_rows))} from {best_rung}:")
            for rank, row in enumerate(best_rows[: args.hpo_top], start=1):
                overrides = row.get("overrides") or {}
                short = ", ".join(f"{key}={value}" for key, value in sorted(overrides.items())[:6])
                print(f"  {rank}. adp={fmt(row.get(METRIC), 3)} auc={fmt(row.get('auc'), 3)} ap={fmt(row.get('ap'), 3)} ckpt={'yes' if checkpoint_ok(row) else 'no'} name={row.get('name')} {short}")
        else:
            print("leaderboard: no completed HPO rows yet")


def remote_final(args):
    repo = Path(args.repo)
    suffix = args.run_suffix.strip("_")
    print("PIDSMaker final e12 results")
    print(f"Repo/storage path: {repo}")
    if suffix:
        print(f"Run suffix: {suffix}")
    print("Rows are final fresh 12-epoch runs. source_rank/source_adp refer to the source HPO rung that selected the final run.\n")

    rows = []
    missing = []
    for method in FINAL_METHODS:
        run_root = asha_root(repo) / run_namespace(method, suffix)
        state = read_json(run_root / "state.json")
        final_state = state.get("final") or {}
        planned = final_state.get("planned") or []
        source_rung = final_state.get("source_rung") or "r3_e9"
        source_rows, _ = load_rows(repo, method, suffix, source_rung, "hpo")
        source_by_name = {str(row.get("name")): (rank, row) for rank, row in enumerate(source_rows, start=1)}
        final_rows, _ = load_rows(repo, method, suffix, "final_e12", "final")
        final_by_name = {str(row.get("name")): row for row in final_rows}
        for rank, name in enumerate(planned, start=1):
            row = final_by_name.get(str(name))
            if not row:
                missing.append(f"{method}:{name}")
                continue
            source_rank, source_row = source_by_name.get(str(name), ("-", {}))
            overrides = row.get("overrides") or {}
            rows.append(
                {
                    "method": method,
                    "final_rank": rank,
                    "source_rank": source_rank,
                    "source_rung": source_rung,
                    "config": name,
                    "adp": fmt(row.get("adp_score"), 3),
                    "auc": fmt(row.get("auc"), 3),
                    "ap": fmt(row.get("ap"), 3),
                    "source_adp": fmt(source_row.get("adp_score"), 3),
                    "ckpt": "yes" if checkpoint_ok(row) else "no",
                    "lr": overrides.get("training.lr", "-"),
                    "weight_decay": overrides.get("training.weight_decay", "-"),
                    "train_seed": overrides.get("training.seed", "-"),
                }
            )
    if rows:
        table(rows, [("method", "method"), ("final_rank", "final_rank"), ("source_rank", "source_rank"), ("source_rung", "source_rung"), ("config", "config"), ("adp", "adp"), ("auc", "auc"), ("ap", "ap"), ("source_adp", "source_adp"), ("ckpt", "ckpt"), ("lr", "lr"), ("weight_decay", "weight_decay"), ("train_seed", "train_seed")])
    else:
        print("No final rows found.")
    if missing:
        print("\nMissing planned final results: " + ", ".join(missing))


def remote_jobs_json(args):
    proc = run_cmd(["squeue", "-u", env("USER", ""), "-h", "-o", "%i|%P|%j|%T|%M|%L|%D|%R"])
    jobs = []
    for line in sorted(proc.stdout.splitlines(), reverse=True):
        parts = line.split("|", 7)
        if len(parts) == 8:
            job_id, partition, name, state, elapsed, left, nodes, reason = [part.strip() for part in parts]
            jobs.append({"job_id": job_id, "partition": partition, "name": name, "state": state, "elapsed": elapsed, "left": left, "nodes": nodes, "reason": reason})
    print(json.dumps(jobs))


def remote_job_view(args):
    job_id = args.job_id or ""
    job_name = args.job_name or ""
    out = Path(args.repo) / "run_logs" / f"{job_name}-{job_id}.out"
    err = out.with_suffix(".err")
    action = args.action
    if action == "job-full":
        print(run_cmd(["squeue", "-j", job_id, "-o", "%.18i %.9P %.24j %.2t %.12M %.12L %.6D %R"]).stdout, end="")
        action = "job-progress"
    if action == "job-progress":
        if not out.exists():
            print(f"Missing stdout log: {out}")
            return
        pattern = re.compile(r"\[[0-9]+/[0-9]+\]|ASHA status|account_capacity|submit rung|Submitted batch job|planned=|adp_score|completed|wrote|Wrote|Error|error|Traceback|MemoryError")
        lines = [line.rstrip("\n") for line in out.read_text(errors="replace").splitlines() if pattern.search(line)]
        print("\n".join(lines[-args.lines:]))
    elif action == "job-logs":
        if out.exists():
            print("\n".join(out.read_text(errors="replace").splitlines()[-args.lines:]))
        else:
            print(f"Missing stdout log: {out}")
    elif action == "job-errors":
        if err.exists() and err.stat().st_size:
            print("\n".join(err.read_text(errors="replace").splitlines()[-args.lines:]))
        else:
            print(f"stderr log is empty or missing: {err}")
    elif action == "job-size":
        for rel in ["capture_export/pidsmaker_export", "capture_export/reference_dataset", "run_logs"]:
            full = Path(args.repo) / rel
            if full.exists():
                print(run_cmd(["du", "-sh", str(full)]).stdout, end="")
        print(run_cmd(["df", "-h", args.repo]).stdout, end="")
    elif action == "job-memory":
        node = run_cmd(["squeue", "-j", job_id, "-h", "-o", "%N"]).stdout.strip()
        if not node or node == "(None)":
            print("Job is not running; no live memory available.")
            return
        command = "ps -u \"$USER\" -o pid=,ppid=,pcpu=,rss=,etime=,comm= --sort=-rss | head -30"
        print(run_cmd(["ssh", node, command]).stdout, end="")


def select_run(args, force=False):
    if args.run_selected and not force:
        return
    if not sys.stdin.isatty():
        return
    runs = json.loads(run_remote(args, "list-runs", capture=True) or "[]")
    if not runs:
        print(f"No ASHA run namespaces found under {args.repo}/meluxina/pidsmaker/asha_runs.")
        return
    if len(runs) == 1 and not force:
        args.run_suffix = runs[0]["suffix"]
        args.run_selected = True
        return
    print("\nSelect ASHA run namespace")
    print("Current: " + (args.run_suffix or "original/default"))
    for index, item in enumerate(runs, start=1):
        print(f"{index}) {item['display']}  [{item['details']}]")
    print(f"{len(runs) + 1}) Quit")
    while True:
        choice = input("Select ASHA run: ").strip()
        if choice == str(len(runs) + 1):
            raise KeyboardInterrupt
        try:
            item = runs[int(choice) - 1]
        except (ValueError, IndexError):
            print("Invalid selection.")
            continue
        args.run_suffix = item["suffix"]
        args.run_selected = True
        print("Using ASHA run namespace: " + (args.run_suffix or "original/default"))
        return


def local_action(args, action):
    if action in {"summary", "asha", "hpo", "final", "finals", "final-results"}:
        select_run(args)
    remote_action = {"asha": "summary", "finals": "final", "final-results": "final"}.get(action, action)
    run_remote(args, remote_action)


def job_details(args):
    while True:
        jobs = json.loads(run_remote(args, "jobs-json", capture=True) or "[]")
        if not jobs:
            print("No queued or running MeluXina jobs found.")
            return
        print("\nCurrent MeluXina jobs:")
        for index, job in enumerate(jobs, start=1):
            print(f"{index}) job={job['job_id']} partition={job['partition']} name={job['name']} state={job['state']} elapsed={job['elapsed']} remaining={job['left']} nodes={job['nodes']} node/reason={job['reason']}")
        print(f"{len(jobs) + 1}) Main menu")
        choice = input("Select job number: ").strip()
        if choice == str(len(jobs) + 1):
            return
        try:
            job = jobs[int(choice) - 1]
        except (ValueError, IndexError):
            print("Invalid selection.")
            continue
        while True:
            views = ["Full", "Progress", "Logs", "Errors", "Memory", "Size", "Other job", "Main menu", "Quit"]
            print(f"\nSelected {job['job_id']} ({job['name']}).")
            for index, view in enumerate(views, start=1):
                print(f"{index}) {view}")
            view_choice = input("Select view: ").strip()
            try:
                view = views[int(view_choice) - 1]
            except (ValueError, IndexError):
                print("Invalid selection.")
                continue
            if view == "Quit":
                raise KeyboardInterrupt
            if view == "Main menu":
                return
            if view == "Other job":
                break
            action = "job-" + view.lower().replace(" ", "-")
            print(f"\n== {view} ==")
            run_remote(args, action, job_id=job["job_id"], job_name=job["name"])


def menu(args):
    while True:
        print("\nMeluXina Job Status")
        print("ASHA run: " + (args.run_suffix or "original/default"))
        actions = ["Summary", "HPO detail", "Final results", "Select ASHA run", "Job details", "Quit"]
        for index, action in enumerate(actions, start=1):
            print(f"{index}) {action}")
        choice = input("Select action: ").strip()
        try:
            action = actions[int(choice) - 1]
        except (ValueError, IndexError):
            print("Invalid selection.")
            continue
        try:
            if action == "Quit":
                return
            if action == "Select ASHA run":
                select_run(args, force=True)
            elif action == "Job details":
                job_details(args)
            elif action == "Summary":
                local_action(args, "summary")
            elif action == "HPO detail":
                local_action(args, "hpo")
            elif action == "Final results":
                local_action(args, "final")
        except KeyboardInterrupt:
            print("")
            return


def parse_args(argv):
    lines_default = env("MELUXINA_STATUS_LINES", "40") or "40"
    hpo_top_default = env("MELUXINA_HPO_TOP", "5") or "5"
    run_suffix_default = (env("MELUXINA_ASHA_RUN_SUFFIX", "") or "").strip().strip("_")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", action="store_true")
    parser.add_argument("--repo", default=env("MELUXINA_REPO", DEFAULT_REPO))
    parser.add_argument("--ssh-host", default=env("MELUXINA_SSH_HOST", "u101059@login.lxp.lu"))
    parser.add_argument("--ssh-key", default=env("MELUXINA_SSH_KEY", "/home/omar/.ssh/meluxina"))
    parser.add_argument("--ssh-port", default=env("MELUXINA_SSH_PORT", "8822"))
    parser.add_argument("--ssh-connect-timeout", default=env("MELUXINA_SSH_CONNECT_TIMEOUT", "15"))
    parser.add_argument("--lines", type=int, default=int(lines_default))
    parser.add_argument("--hpo-top", type=int, default=int(hpo_top_default))
    parser.add_argument("--run-suffix", default=run_suffix_default)
    parser.add_argument("action", nargs="?", default="menu")
    parser.add_argument("--job-id")
    parser.add_argument("--job-name")
    args = parser.parse_args(argv)
    args.run_selected = bool(args.run_suffix)
    return args


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.remote:
            action = str({"asha": "summary", "finals": "final", "final-results": "final"}.get(args.action, args.action))
            if action == "list-runs":
                remote_list_runs(args)
            elif action == "summary":
                remote_summary(args)
            elif action == "hpo":
                remote_hpo(args)
            elif action == "final":
                remote_final(args)
            elif action == "jobs-json":
                remote_jobs_json(args)
            elif action.startswith("job-"):
                remote_job_view(args)
            else:
                raise SystemExit(f"Unknown remote action: {args.action}")
        elif args.action == "menu":
            menu(args)
        else:
            local_action(args, args.action)
    except KeyboardInterrupt:
        print("")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
