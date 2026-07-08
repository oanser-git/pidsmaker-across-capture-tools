#!/usr/bin/env python3
# pyright: reportAttributeAccessIssue=false, reportArgumentType=false, reportAssignmentType=false, reportCallIssue=false, reportGeneralTypeIssues=false, reportIndexIssue=false
"""Asynchronous Successive Halving controller for Slurm-backed PIDSMaker HPO.

The controller is intentionally small and Slurm-native: it submits existing HPO
array jobs, watches the per-task JSON files written by run_hpo_apptainer_task.py,
and promotes configurations to the next rung as soon as enough results exist.

This file is Python 3.6-compatible because the Iris login python is 3.6.8.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml


STATE_VERSION = 1
PID_PHASE = "hpo"
VALID_MODES = {"minimize", "maximize"}
VALID_PROMOTION_POLICIES = {"async", "sync"}
DEFAULT_METRIC = "adp_score"
DEFAULT_MODE = "maximize"


class RungConfig(object):
    def __init__(
        self,
        name,
        sweep,
        results_dir,
        tag,
        job_name,
        sbatch_script,
        array_concurrency,
        sbatch_options,
        export_env,
    ):
        self.name = name
        self.sweep = sweep
        self.results_dir = results_dir
        self.tag = tag
        self.job_name = job_name
        self.sbatch_script = sbatch_script
        self.array_concurrency = array_concurrency
        self.sbatch_options = sbatch_options
        self.export_env = export_env


class AshaConfig(object):
    def __init__(
        self,
        name,
        config_path,
        state_path,
        results_root,
        metric,
        mode,
        reduction_factor,
        promotion_policy,
        poll_seconds,
        start_trials,
        rungs,
    ):
        self.name = name
        self.config_path = config_path
        self.state_path = state_path
        self.results_root = results_root
        self.metric = metric
        self.mode = mode
        self.reduction_factor = reduction_factor
        self.promotion_policy = promotion_policy
        self.poll_seconds = poll_seconds
        self.start_trials = start_trials
        self.rungs = rungs


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def workspace_root():
    return Path(os.environ.get("P_EDR_ROOT", Path(__file__).resolve().parents[2])).resolve()


def default_vars():
    root = workspace_root()
    export_root = Path(os.environ.get("ORANGE_EXPORT_ROOT", root / "capture_export" / "pidsmaker_export")).resolve()
    return {
        "P_EDR_ROOT": str(root),
        "ORANGE_EXPORT_ROOT": str(export_root),
    }


def expand(value, variables):
    if isinstance(value, str):
        for key, replacement in variables.items():
            value = value.replace("${" + key + "}", replacement)
    elif isinstance(value, list):
        value = [expand(item, variables) for item in value]
    elif isinstance(value, dict):
        value = {key: expand(item, variables) for key, item in value.items()}
    return value


def load_yaml(path):
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError("YAML file must be a mapping: {}".format(path))
    return payload


def safe_name(value, max_length=80):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_.-")[:max_length]


def shell_join(command):
    return " ".join(shlex.quote(str(part)) for part in command)


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def resolve_path(value, base=None):
    path = Path(str(value))
    if path.is_absolute():
        return path
    if base is None:
        base = Path.cwd()
    return (base / path).resolve()


def load_sweep_runs(sweep_path):
    variables = default_vars()
    sweep = expand(load_yaml(sweep_path), variables)
    runs = list(sweep.get("runs") or [])
    if not runs and "run_templates" in sweep:
        for template in sweep["run_templates"]:
            base_overrides = dict(template.get("overrides") or {})
            for seed in sweep.get("seeds", [None]):
                overrides = dict(base_overrides)
                suffix = ""
                if seed is not None:
                    suffix = "_seed{}".format(seed)
                    overrides.setdefault("training.seed", seed)
                    overrides.setdefault("featurization.seed", seed)
                runs.append({"name": "{}{}".format(template["name"], suffix), "overrides": overrides})
    for index, run in enumerate(runs):
        if "name" not in run:
            raise ValueError("Run at index {} in {} has no name".format(index, sweep_path))
    return runs


def parse_array_limit(value):
    if value in (None, ""):
        return None
    limit = int(value)
    if limit <= 0:
        raise ValueError("array concurrency must be positive")
    return limit


def array_spec(indices, concurrency):
    if not indices:
        raise ValueError("Cannot build Slurm array spec with no indices")
    sorted_indices = sorted(indices)
    ranges = []
    start = sorted_indices[0]
    previous = sorted_indices[0]
    for index in sorted_indices[1:]:
        if index == previous + 1:
            previous = index
            continue
        ranges.append("{}-{}".format(start, previous) if start != previous else str(start))
        start = index
        previous = index
    ranges.append("{}-{}".format(start, previous) if start != previous else str(start))
    spec = ",".join(ranges)
    if concurrency:
        spec = "{}%{}".format(spec, concurrency)
    return spec


def metric_value(row, metric):
    value = row.get(metric)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def result_is_complete(row):
    return str(row.get("phase", "")) == PID_PHASE


def result_is_promotable(row, metric):
    if not result_is_complete(row):
        return False
    if int(row.get("exit_code", 1) or 0) != 0:
        return False
    if bool(row.get("oom")):
        return False
    return metric_value(row, metric) is not None


def build_rung_config(
    raw,
    config_dir,
    results_root,
    default_sbatch_script,
    default_array_concurrency,
    default_sbatch_options,
    default_export_env,
):
    name = safe_name(str(raw["name"]))
    sweep = resolve_path(str(raw["sweep"]), config_dir)
    results_dir = resolve_path(str(raw.get("results_dir", results_root / name)), config_dir)
    tag = safe_name(str(raw.get("tag") or name))
    job_name = safe_name(str(raw.get("job_name") or "asha_{}".format(name)), max_length=60)
    sbatch_script = resolve_path(str(raw.get("sbatch_script") or default_sbatch_script), config_dir)
    array_concurrency = parse_array_limit(raw.get("array_concurrency", default_array_concurrency))

    sbatch_options = dict(default_sbatch_options)
    sbatch_options.update(dict(raw.get("sbatch_options") or {}))
    export_env = dict(default_export_env)
    export_env.update(dict(raw.get("export_env") or {}))

    return RungConfig(
        name,
        sweep,
        results_dir,
        tag,
        job_name,
        sbatch_script,
        array_concurrency,
        sbatch_options,
        export_env,
    )


def load_config(path):
    path = path.resolve()
    variables = default_vars()
    raw = expand(load_yaml(path), variables)
    name = safe_name(str(raw["name"]))
    default_results_root = workspace_root() / "pidsmaker_experiments" / "asha_runs" / name
    results_root = resolve_path(str(raw.get("results_root", default_results_root)), path.parent)
    state_path = resolve_path(str(raw.get("state_path", results_root / "state.json")), path.parent)
    metric = str(raw.get("metric", DEFAULT_METRIC))
    mode = str(raw.get("mode", DEFAULT_MODE)).lower()
    if mode not in VALID_MODES:
        raise ValueError("mode must be one of {}, got {!r}".format(sorted(VALID_MODES), mode))
    reduction_factor = int(raw.get("reduction_factor", 3))
    if reduction_factor < 2:
        raise ValueError("reduction_factor must be >= 2")
    promotion_policy = str(raw.get("promotion_policy", "async")).lower()
    if promotion_policy not in VALID_PROMOTION_POLICIES:
        raise ValueError(
            "promotion_policy must be one of {}, got {!r}".format(
                sorted(VALID_PROMOTION_POLICIES), promotion_policy
            )
        )
    poll_seconds = int(raw.get("poll_seconds", 60))
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")

    default_script = workspace_root() / "pidsmaker_experiments" / "hpc" / "run_hpo_apptainer_array.sbatch"
    default_sbatch_script = resolve_path(str(raw.get("sbatch_script", default_script)), path.parent)
    default_array_concurrency = parse_array_limit(raw.get("array_concurrency"))
    default_sbatch_options = dict(raw.get("sbatch_options") or {})
    default_export_env = dict(raw.get("export_env") or {})

    rungs_raw = list(raw.get("rungs") or [])
    if len(rungs_raw) < 2:
        raise ValueError("ASHA config needs at least two rungs")
    rungs = [
        build_rung_config(
            dict(rung),
            path.parent,
            results_root,
            default_sbatch_script,
            default_array_concurrency,
            default_sbatch_options,
            default_export_env,
        )
        for rung in rungs_raw
    ]

    start_trials = [str(item) for item in as_list(raw.get("start_trials"))] or None
    return AshaConfig(
        name,
        path,
        state_path,
        results_root,
        metric,
        mode,
        reduction_factor,
        promotion_policy,
        poll_seconds,
        start_trials,
        rungs,
    )


def initial_state(config):
    return {
        "version": STATE_VERSION,
        "name": config.name,
        "config_path": str(config.config_path),
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "metric": config.metric,
        "mode": config.mode,
        "reduction_factor": config.reduction_factor,
        "promotion_policy": config.promotion_policy,
        "rungs": {
            rung.name: {
                "submitted": [],
                "promoted": [],
                "cancelled": [],
                "submissions": [],
            }
            for rung in config.rungs
        },
    }


def load_state(config):
    if not config.state_path.exists():
        return initial_state(config)
    state = json.loads(config.state_path.read_text(encoding="utf-8"))
    if int(state.get("version", 0)) != STATE_VERSION:
        raise ValueError("Unsupported state version in {}: {}".format(config.state_path, state.get("version")))
    state.setdefault("rungs", {})
    for rung in config.rungs:
        state["rungs"].setdefault(rung.name, {"submitted": [], "promoted": [], "cancelled": [], "submissions": []})
        state["rungs"][rung.name].setdefault("submitted", [])
        state["rungs"][rung.name].setdefault("promoted", [])
        state["rungs"][rung.name].setdefault("cancelled", [])
        state["rungs"][rung.name].setdefault("submissions", [])
    return state


def save_state(config, state):
    state["updated_at"] = utc_now()
    config.state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = config.state_path.with_name(".{}.tmp".format(config.state_path.name))
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(tmp_path), str(config.state_path))


def set_union_preserve_order(existing, additions):
    seen = set(existing)
    merged = list(existing)
    for item in additions:
        if item not in seen:
            merged.append(item)
            seen.add(item)
    return merged


def load_result_rows(rung):
    rows = {}
    if not rung.results_dir.exists():
        return rows
    for path in sorted(rung.results_dir.glob("*.json")):
        if path.name in {"results.json", "leaderboard.json", "asha_status.json"}:
            continue
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        name = str(row.get("name") or "")
        if name:
            rows[name] = row
    return rows


def rank_rows(rows, metric, mode):
    reverse = mode == "maximize"

    def score(row):
        value = metric_value(row, metric)
        if value is None:
            return float("-inf") if reverse else float("inf")
        return value

    return sorted(rows, key=score, reverse=reverse)


def run_index_by_name(rung):
    runs = load_sweep_runs(rung.sweep)
    mapping = {}
    for index, run in enumerate(runs):
        name = safe_name(str(run["name"]))
        if name in mapping:
            raise ValueError("Duplicate safe run name {!r} in {}".format(name, rung.sweep))
        mapping[name] = index
    return mapping


def build_sbatch_command(rung, names, indices, checkpoint_root=None, resume_from_rung=None):
    variables = default_vars()
    export_items = {
        "P_EDR_ROOT": variables["P_EDR_ROOT"],
        "ORANGE_EXPORT_ROOT": variables["ORANGE_EXPORT_ROOT"],
        "HPO_SWEEP": str(rung.sweep),
        "HPO_TAG": rung.tag,
        "HPO_RESULTS_DIR": str(rung.results_dir),
    }
    if checkpoint_root is not None:
        export_items["HPO_CHECKPOINT_ROOT"] = str(checkpoint_root)
    if resume_from_rung is not None:
        export_items["HPO_RESUME_FROM_SWEEP"] = str(resume_from_rung.sweep)
        export_items["HPO_RESUME_FROM_TAG"] = str(resume_from_rung.tag)
        export_items["HPO_RESUME_FROM_PHASE"] = PID_PHASE
    export_items.update({str(key): str(value) for key, value in rung.export_env.items()})
    export_arg = "ALL," + ",".join("{}={}".format(key, value) for key, value in export_items.items())

    command = [
        "sbatch",
        "--array={}".format(array_spec(indices, rung.array_concurrency)),
        "--job-name={}".format(rung.job_name),
    ]
    sbatch_key_map = {
        "mem": "--mem",
        "time": "--time",
        "constraint": "--constraint",
        "partition": "--partition",
        "qos": "--qos",
        "gres": "--gres",
        "cpus_per_task": "--cpus-per-task",
        "account": "--account",
    }
    for key, flag in sbatch_key_map.items():
        value = rung.sbatch_options.get(key)
        if value not in (None, ""):
            command.append("{}={}".format(flag, value))
    for raw_arg in as_list(rung.sbatch_options.get("extra_args")):
        command.append(str(raw_arg))
    command.extend(["--export={}".format(export_arg), str(rung.sbatch_script)])
    return command


def submit_names(rung, names, dry_run, checkpoint_root=None, resume_from_rung=None):
    if not names:
        return None, [], []
    mapping = run_index_by_name(rung)
    missing = [name for name in names if name not in mapping]
    if missing:
        raise ValueError("Promoted run(s) missing from {}: {}".format(rung.sweep, missing))
    indices = [mapping[name] for name in names]
    command = build_sbatch_command(
        rung,
        names,
        indices,
        checkpoint_root=checkpoint_root,
        resume_from_rung=resume_from_rung,
    )
    print("submit rung={} names={} indices={}".format(rung.name, len(names), indices), flush=True)
    print("$ " + shell_join(command), flush=True)
    if dry_run:
        return None, indices, command
    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    print(output.strip(), flush=True)
    match = re.search(r"Submitted batch job\s+(\d+)", output)
    if not match:
        raise RuntimeError("Could not parse sbatch job id from output: {!r}".format(output))
    return match.group(1), indices, command


def record_submission(state, rung, names, job_id, indices, command):
    rung_state = state["rungs"][rung.name]
    rung_state["submitted"] = set_union_preserve_order(list(rung_state.get("submitted") or []), names)
    rung_state["submissions"].append(
        {
            "time": utc_now(),
            "job_id": job_id,
            "names": names,
            "indices": indices,
            "command": command,
        }
    )


def record_existing_results(state, rung, names):
    if not names:
        return
    mapping = run_index_by_name(rung)
    indices = [mapping[name] for name in names if name in mapping]
    rung_state = state["rungs"][rung.name]
    rung_state["submitted"] = set_union_preserve_order(list(rung_state.get("submitted") or []), names)
    rung_state["submissions"].append(
        {
            "time": utc_now(),
            "job_id": "existing_result",
            "names": names,
            "indices": indices,
            "command": ["existing_result"],
        }
    )


def write_status(config, state):
    status = {
        "name": config.name,
        "time": utc_now(),
        "metric": config.metric,
        "mode": config.mode,
        "reduction_factor": config.reduction_factor,
        "promotion_policy": config.promotion_policy,
        "rungs": [],
    }
    for rung in config.rungs:
        rung_state = state["rungs"][rung.name]
        submitted = list(rung_state.get("submitted") or [])
        promoted = list(rung_state.get("promoted") or [])
        cancelled = list(rung_state.get("cancelled") or [])
        rows = load_result_rows(rung)
        completed_names = [name for name in submitted if name in rows]
        valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
        ranked = rank_rows(valid_rows, config.metric, config.mode)
        status["rungs"].append(
            {
                "name": rung.name,
                "sweep": str(rung.sweep),
                "results_dir": str(rung.results_dir),
                "submitted": len(submitted),
                "completed": len(completed_names),
                "valid": len(valid_rows),
                "promoted": len(promoted),
                "cancelled": len(cancelled),
                "best": ranked[0].get("name") if ranked else None,
                "best_metric": metric_value(ranked[0], config.metric) if ranked else None,
            }
        )
    config.results_root.mkdir(parents=True, exist_ok=True)
    status_path = config.results_root / "asha_status.json"
    status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return status


def print_status(status):
    print(
        "ASHA status {} metric={} mode={} promotion_policy={}".format(
            status["name"], status["metric"], status["mode"], status.get("promotion_policy", "async")
        ),
        flush=True,
    )
    for rung in status["rungs"]:
        best_text = ""
        if rung.get("best") is not None:
            best_text = " best={} {}={}".format(rung["best"], status["metric"], rung["best_metric"])
        print(
            "  {name}: submitted={submitted} completed={completed} valid={valid} promoted={promoted} cancelled={cancelled}{best_text}".format(
                best_text=best_text,
                **rung
            ),
            flush=True,
        )


def initial_trial_names(config):
    first_rung_names = list(run_index_by_name(config.rungs[0]).keys())
    if config.start_trials is None:
        return first_rung_names
    missing = [name for name in config.start_trials if safe_name(name) not in first_rung_names]
    if missing:
        raise ValueError("start_trials missing from first rung sweep: {}".format(missing))
    keep = {safe_name(name) for name in config.start_trials}
    return [name for name in first_rung_names if name in keep]


def promotion_target(completed_count, submitted_count, reduction_factor):
    if submitted_count <= 0:
        return 0
    target = completed_count // reduction_factor
    if completed_count == submitted_count and completed_count > 0 and target == 0:
        target = 1
    return min(target, max_promotion_count(submitted_count, reduction_factor))


def max_promotion_count(submitted_count, reduction_factor):
    if submitted_count <= 0:
        return 0
    return max(1, submitted_count // reduction_factor)


def sync_promotion_target(ranked_count, submitted_count, reduction_factor):
    return min(ranked_count, max_promotion_count(submitted_count, reduction_factor))


def submitted_index_by_name(rung_state):
    mapping = {}
    for submission in list(rung_state.get("submissions") or []):
        names = list(submission.get("names") or [])
        indices = list(submission.get("indices") or [])
        job_id = submission.get("job_id")
        if not job_id:
            continue
        for name, index in zip(names, indices):
            mapping[str(name)] = (str(job_id), int(index))
    return mapping


def slurm_state(job_token):
    completed = subprocess.run(
        ["squeue", "-h", "-j", job_token, "-o", "%T"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    states = [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]
    return states[0] if states else None


def cancel_pending_tokens(job_tokens, dry_run):
    cancelled = []
    for token in job_tokens:
        state = slurm_state(token)
        if state != "PENDING":
            continue
        print("cancel pending {}".format(token), flush=True)
        if not dry_run:
            subprocess.run(["scancel", token], check=False)
        cancelled.append(token)
    return cancelled


def maybe_cancel_remaining_async(config, state, rung, rows, dry_run):
    rung_state = state["rungs"][rung.name]
    submitted = list(rung_state.get("submitted") or [])
    promoted = list(rung_state.get("promoted") or [])
    if len(promoted) < max_promotion_count(len(submitted), config.reduction_factor):
        return False

    completed = set(rows.keys())
    keep = set(promoted) | completed
    cancelled_names = set(rung_state.get("cancelled") or [])
    index_by_name = submitted_index_by_name(rung_state)
    tokens = []
    token_names = []
    for name in submitted:
        if name in keep or name in cancelled_names:
            continue
        job_info = index_by_name.get(name)
        if not job_info:
            continue
        job_id, index = job_info
        tokens.append("{}_{}".format(job_id, index))
        token_names.append(name)
    if not tokens:
        return False

    cancelled_tokens = set(cancel_pending_tokens(tokens, dry_run))
    if not cancelled_tokens:
        return False
    cancelled = list(rung_state.get("cancelled") or [])
    for name, token in zip(token_names, tokens):
        if token in cancelled_tokens and name not in cancelled:
            cancelled.append(name)
    rung_state["cancelled"] = cancelled
    return True


def maybe_submit_initial(config, state, dry_run):
    first = config.rungs[0]
    first_state = state["rungs"][first.name]
    if first_state.get("submitted"):
        return False
    names = initial_trial_names(config)
    if not names:
        raise ValueError("No initial trials selected")
    job_id, indices, command = submit_names(
        first,
        names,
        dry_run=dry_run,
        checkpoint_root=config.results_root / "checkpoints",
    )
    if not dry_run:
        record_submission(state, first, names, job_id, indices, command)
    return True


def maybe_promote(config, state, dry_run):
    changed = False
    for rung_index, rung in enumerate(config.rungs[:-1]):
        next_rung = config.rungs[rung_index + 1]
        rung_state = state["rungs"][rung.name]
        next_state = state["rungs"][next_rung.name]
        submitted = list(rung_state.get("submitted") or [])
        if not submitted:
            continue

        rows = load_result_rows(rung)
        completed_names = [name for name in submitted if name in rows]
        completed_count = len(completed_names)
        if config.promotion_policy == "sync":
            if completed_count < len(submitted):
                continue
            valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
            ranked = rank_rows(valid_rows, config.metric, config.mode)
            target = sync_promotion_target(len(ranked), len(submitted), config.reduction_factor)
        else:
            target = promotion_target(completed_count, len(submitted), config.reduction_factor)
            valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
            ranked = rank_rows(valid_rows, config.metric, config.mode)
        already_promoted = list(rung_state.get("promoted") or [])
        remaining_slots = target - len(already_promoted)
        if config.promotion_policy == "async" and len(already_promoted) >= max_promotion_count(len(submitted), config.reduction_factor):
            changed = maybe_cancel_remaining_async(config, state, rung, rows, dry_run) or changed
        if remaining_slots <= 0:
            continue

        already_promoted_set = set(already_promoted)
        next_submitted_set = set(next_state.get("submitted") or [])
        next_rows = load_result_rows(next_rung)
        next_valid_set = {
            name for name, row in next_rows.items() if result_is_promotable(row, config.metric)
        }
        candidates = [
            str(row["name"])
            for row in ranked
            if str(row.get("name")) not in already_promoted_set
            and (
                str(row.get("name")) not in next_submitted_set
                or str(row.get("name")) in next_valid_set
            )
        ]
        promote = candidates[:remaining_slots]
        if not promote:
            continue

        reuse = [name for name in promote if name in next_valid_set and name not in next_submitted_set]
        submit = [name for name in promote if name not in next_valid_set]
        if reuse:
            print("reuse rung={} existing_results={} names={}".format(next_rung.name, len(reuse), reuse), flush=True)
        job_id = None
        indices = []
        command = []
        if submit:
            job_id, indices, command = submit_names(
                next_rung,
                submit,
                dry_run=dry_run,
                checkpoint_root=config.results_root / "checkpoints",
                resume_from_rung=rung,
            )
        if not dry_run:
            rung_state["promoted"] = set_union_preserve_order(already_promoted, promote)
            if reuse:
                record_existing_results(state, next_rung, reuse)
            if submit:
                record_submission(state, next_rung, submit, job_id, indices, command)
            if config.promotion_policy == "async" and len(rung_state["promoted"]) >= max_promotion_count(len(submitted), config.reduction_factor):
                changed = maybe_cancel_remaining_async(config, state, rung, rows, dry_run) or changed
        changed = True
    return changed


def all_done(config, state):
    final_rung = config.rungs[-1]
    final_state = state["rungs"][final_rung.name]
    final_submitted = list(final_state.get("submitted") or [])
    if not final_submitted:
        return False
    rows = load_result_rows(final_rung)
    return all(name in rows for name in final_submitted)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print sbatch commands without submitting or updating state")
    parser.add_argument("--poll-seconds", type=int, help="Override config poll interval")
    parser.add_argument("--max-loops", type=int, help="Stop after this many polling iterations")
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)
    poll_seconds = args.poll_seconds or config.poll_seconds
    state = load_state(config)

    loops = 0
    while True:
        loops += 1
        changed = maybe_submit_initial(config, state, dry_run=args.dry_run)
        changed = maybe_promote(config, state, dry_run=args.dry_run) or changed
        if changed and not args.dry_run:
            save_state(config, state)
        status = write_status(config, state)
        print_status(status)

        if args.dry_run or args.once or all_done(config, state):
            if all_done(config, state):
                print("ASHA completed all submitted final-rung trials", flush=True)
            return
        if args.max_loops is not None and loops >= args.max_loops:
            return
        time.sleep(poll_seconds)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR: {}: {}".format(type(exc).__name__, exc), file=sys.stderr, flush=True)
        raise
