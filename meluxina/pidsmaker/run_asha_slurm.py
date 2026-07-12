#!/usr/bin/env python3
"""ASHA controller for MeluXina PIDSMaker Slurm arrays."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
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
DEFAULT_METRIC = "adp_score"
DEFAULT_MODE = "maximize"
VALID_MODES = {"minimize", "maximize"}
VALID_PROMOTION_POLICIES = {"async", "sync"}
ACTIVE_SLURM_STATES = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "REQUEUED", "RESIZING"}


class SubmitLimitReached(RuntimeError):
    pass


class RungConfig:
    def __init__(
        self,
        name: str,
        sweep: Path,
        results_dir: Path,
        tag: str,
        job_name: str,
        sbatch_script: Path,
        array_concurrency: Optional[int],
        submit_accounts: List[str],
        submit_chunk_size: Optional[int],
        max_submit_jobs_per_account: Optional[int],
        sbatch_options: Dict[str, Any],
        export_env: Dict[str, Any],
    ) -> None:
        self.name = name
        self.sweep = sweep
        self.results_dir = results_dir
        self.tag = tag
        self.job_name = job_name
        self.sbatch_script = sbatch_script
        self.array_concurrency = array_concurrency
        self.submit_accounts = submit_accounts
        self.submit_chunk_size = submit_chunk_size
        self.max_submit_jobs_per_account = max_submit_jobs_per_account
        self.sbatch_options = sbatch_options
        self.export_env = export_env


class FinalConfig:
    def __init__(self, source_rung: str, top_k: int, rung: RungConfig) -> None:
        self.source_rung = source_rung
        self.top_k = top_k
        self.rung = rung


class AshaConfig:
    def __init__(
        self,
        name: str,
        config_path: Path,
        state_path: Path,
        results_root: Path,
        metric: str,
        mode: str,
        reduction_factor: int,
        promotion_policy: str,
        poll_seconds: int,
        start_trials: Optional[List[str]],
        rungs: List[RungConfig],
        final: Optional[FinalConfig],
    ) -> None:
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
        self.final = final


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def workspace_root() -> Path:
    return Path(os.environ.get("P_EDR_ROOT", Path(__file__).resolve().parents[2])).resolve()


def default_vars() -> Dict[str, str]:
    root = workspace_root()
    export_root = Path(
        os.environ.get(
            "ORANGE_EXPORT_ROOT",
            "/mnt/tier2/project/p201223/pidsmaker-across-capture-tools/capture_export/pidsmaker_export",
        )
    ).resolve()
    return {"P_EDR_ROOT": str(root), "ORANGE_EXPORT_ROOT": str(export_root)}


def expand(value: Any, variables: Dict[str, str]) -> Any:
    if isinstance(value, str):
        for key, replacement in variables.items():
            value = value.replace("${" + key + "}", replacement)
    elif isinstance(value, list):
        value = [expand(item, variables) for item in value]
    elif isinstance(value, dict):
        value = {key: expand(item, variables) for key, item in value.items()}
    return value


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"YAML file must be a mapping: {path}")
    return payload


def safe_name(value: str, max_length: int = 80) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_.-")[:max_length]


def shell_join(command: List[Any]) -> str:
    return " ".join(shlex.quote(str(part)) for part in command)


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def resolve_path(value: Any, base: Path) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    return (base / path).resolve()


def parse_array_limit(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    limit = int(value)
    if limit <= 0:
        raise ValueError("array concurrency must be positive")
    return limit


def parse_positive_int(value: Any, name: str) -> Optional[int]:
    if value in (None, ""):
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def parse_accounts(value: Any) -> List[str]:
    accounts = [str(item) for item in as_list(value) if str(item)]
    return accounts


def array_spec(indices: List[int], concurrency: Optional[int]) -> str:
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
        ranges.append(f"{start}-{previous}" if start != previous else str(start))
        start = index
        previous = index
    ranges.append(f"{start}-{previous}" if start != previous else str(start))
    spec = ",".join(ranges)
    if concurrency:
        spec = f"{spec}%{concurrency}"
    return spec


def load_sweep_runs(sweep_path: Path) -> List[Dict[str, Any]]:
    sweep = expand(load_yaml(sweep_path), default_vars())
    runs = list(sweep.get("runs") or [])
    for index, run in enumerate(runs):
        if "name" not in run:
            raise ValueError(f"Run at index {index} in {sweep_path} has no name")
    return runs


def build_rung_config(
    raw: Dict[str, Any],
    config_dir: Path,
    results_root: Path,
    default_sbatch_script: Path,
    default_array_concurrency: Optional[int],
    default_submit_accounts: List[str],
    default_submit_chunk_size: Optional[int],
    default_max_submit_jobs_per_account: Optional[int],
    default_sbatch_options: Dict[str, Any],
    default_export_env: Dict[str, Any],
) -> RungConfig:
    name = safe_name(str(raw["name"]))
    sweep = resolve_path(raw["sweep"], config_dir)
    results_dir = resolve_path(raw.get("results_dir", results_root / name), config_dir)
    tag = safe_name(str(raw.get("tag") or name))
    job_name = safe_name(str(raw.get("job_name") or f"asha_{name}"), max_length=60)
    sbatch_script = resolve_path(raw.get("sbatch_script") or default_sbatch_script, config_dir)
    array_concurrency = parse_array_limit(raw.get("array_concurrency", default_array_concurrency))

    sbatch_options = dict(default_sbatch_options)
    sbatch_options.update(dict(raw.get("sbatch_options") or {}))
    export_env = dict(default_export_env)
    export_env.update(dict(raw.get("export_env") or {}))
    submit_accounts = parse_accounts(raw.get("submit_accounts", default_submit_accounts))
    if not submit_accounts and sbatch_options.get("account"):
        submit_accounts = [str(sbatch_options["account"])]
    submit_chunk_size = parse_positive_int(raw.get("submit_chunk_size", default_submit_chunk_size), "submit_chunk_size")
    max_submit_jobs_per_account = parse_positive_int(
        raw.get("max_submit_jobs_per_account", default_max_submit_jobs_per_account),
        "max_submit_jobs_per_account",
    )
    return RungConfig(
        name,
        sweep,
        results_dir,
        tag,
        job_name,
        sbatch_script,
        array_concurrency,
        submit_accounts,
        submit_chunk_size,
        max_submit_jobs_per_account,
        sbatch_options,
        export_env,
    )


def load_config(path: Path) -> AshaConfig:
    path = path.resolve()
    raw = expand(load_yaml(path), default_vars())
    name = safe_name(str(raw["name"]))
    default_results_root = workspace_root() / "meluxina" / "pidsmaker" / "asha_runs" / name
    results_root = resolve_path(raw.get("results_root", default_results_root), path.parent)
    state_path = resolve_path(raw.get("state_path", results_root / "state.json"), path.parent)

    metric = str(raw.get("metric", DEFAULT_METRIC))
    mode = str(raw.get("mode", DEFAULT_MODE)).lower()
    if mode not in VALID_MODES:
        raise ValueError(f"mode must be one of {sorted(VALID_MODES)}, got {mode!r}")
    reduction_factor = int(raw.get("reduction_factor", 3))
    if reduction_factor < 2:
        raise ValueError("reduction_factor must be >= 2")
    promotion_policy = str(raw.get("promotion_policy", "sync")).lower()
    if promotion_policy not in VALID_PROMOTION_POLICIES:
        raise ValueError(f"promotion_policy must be one of {sorted(VALID_PROMOTION_POLICIES)}, got {promotion_policy!r}")
    poll_seconds = int(raw.get("poll_seconds", 120))
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")

    default_script = workspace_root() / "meluxina" / "pidsmaker" / "run_array.sbatch"
    default_sbatch_script = resolve_path(raw.get("sbatch_script", default_script), path.parent)
    default_array_concurrency = parse_array_limit(raw.get("array_concurrency"))
    default_sbatch_options = dict(raw.get("sbatch_options") or {})
    default_submit_accounts = parse_accounts(raw.get("submit_accounts"))
    if not default_submit_accounts and default_sbatch_options.get("account"):
        default_submit_accounts = [str(default_sbatch_options["account"])]
    default_submit_chunk_size = parse_positive_int(raw.get("submit_chunk_size"), "submit_chunk_size")
    default_max_submit_jobs_per_account = parse_positive_int(
        raw.get("max_submit_jobs_per_account"),
        "max_submit_jobs_per_account",
    )
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
            default_submit_accounts,
            default_submit_chunk_size,
            default_max_submit_jobs_per_account,
            default_sbatch_options,
            default_export_env,
        )
        for rung in rungs_raw
    ]
    final_config = None  # type: Optional[FinalConfig]
    if raw.get("final"):
        final_raw = dict(raw["final"])
        source_rung = safe_name(str(final_raw.get("source_rung") or rungs[-1].name))
        if source_rung not in {rung.name for rung in rungs}:
            raise ValueError(f"final.source_rung {source_rung!r} is not an ASHA rung")
        top_k = int(final_raw.get("top_k", 3))
        if top_k <= 0:
            raise ValueError("final.top_k must be positive")
        final_rung = build_rung_config(
            final_raw,
            path.parent,
            results_root,
            default_sbatch_script,
            default_array_concurrency,
            default_submit_accounts,
            default_submit_chunk_size,
            default_max_submit_jobs_per_account,
            default_sbatch_options,
            default_export_env,
        )
        final_config = FinalConfig(source_rung, top_k, final_rung)
    start_trials = [safe_name(str(item)) for item in as_list(raw.get("start_trials"))] or None
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
        final_config,
    )


def initial_state(config: AshaConfig) -> Dict[str, Any]:
    state = {
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
            rung.name: {"planned": [], "submitted": [], "promoted": [], "cancelled": [], "submissions": []}
            for rung in config.rungs
        },
    }
    if config.final:
        state["final"] = {
            "source_rung": config.final.source_rung,
            "top_k": config.final.top_k,
            "planned": [],
            "submitted": [],
            "cancelled": [],
            "submissions": [],
        }
    return state


def load_state(config: AshaConfig) -> Dict[str, Any]:
    if not config.state_path.exists():
        return initial_state(config)
    state = json.loads(config.state_path.read_text(encoding="utf-8"))
    if int(state.get("version", 0)) != STATE_VERSION:
        raise ValueError(f"Unsupported state version in {config.state_path}: {state.get('version')}")
    state.setdefault("rungs", {})
    for rung in config.rungs:
        state["rungs"].setdefault(
            rung.name,
            {"planned": [], "submitted": [], "promoted": [], "cancelled": [], "submissions": []},
        )
        state["rungs"][rung.name].setdefault("planned", [])
        state["rungs"][rung.name].setdefault("submitted", [])
        state["rungs"][rung.name].setdefault("promoted", [])
        state["rungs"][rung.name].setdefault("cancelled", [])
        state["rungs"][rung.name].setdefault("submissions", [])
    if config.final:
        state.setdefault("final", {})
        state["final"].setdefault("source_rung", config.final.source_rung)
        state["final"].setdefault("top_k", config.final.top_k)
        state["final"].setdefault("planned", [])
        state["final"].setdefault("submitted", [])
        state["final"].setdefault("cancelled", [])
        state["final"].setdefault("submissions", [])
    return state


def save_state(config: AshaConfig, state: Dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    config.state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = config.state_path.with_name(f".{config.state_path.name}.tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(tmp_path), str(config.state_path))


def set_union_preserve_order(existing: List[str], additions: List[str]) -> List[str]:
    seen = set(existing)
    merged = list(existing)
    for item in additions:
        if item not in seen:
            merged.append(item)
            seen.add(item)
    return merged


def metric_value(row: Dict[str, Any], metric: str) -> Optional[float]:
    value = row.get(metric)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def result_is_promotable(row: Dict[str, Any], metric: str) -> bool:
    if str(row.get("phase", "")) != PID_PHASE:
        return False
    if int(row.get("exit_code", 1) or 0) != 0:
        return False
    if bool(row.get("oom")):
        return False
    return metric_value(row, metric) is not None


def result_has_checkpoint(row: Dict[str, Any]) -> bool:
    path = row.get("checkpoint_path")
    return bool(row.get("checkpoint_saved")) and bool(path) and Path(str(path)).exists()


def rung_requires_resume(rung: RungConfig) -> bool:
    sweep = expand(load_yaml(rung.sweep), default_vars())
    return bool(sweep.get("resume_checkpoint_dir"))


def downstream_requires_checkpoint(config: AshaConfig, rung: RungConfig) -> bool:
    for index, candidate in enumerate(config.rungs[:-1]):
        if candidate.name == rung.name:
            return rung_requires_resume(config.rungs[index + 1])
    return False


def result_is_reusable(config: AshaConfig, rung: RungConfig, row: Dict[str, Any]) -> bool:
    if not result_is_promotable(row, config.metric):
        return False
    if downstream_requires_checkpoint(config, rung) and not result_has_checkpoint(row):
        return False
    return True


def load_result_rows(rung: RungConfig) -> Dict[str, Dict[str, Any]]:
    rows = {}  # type: Dict[str, Dict[str, Any]]
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


def rank_rows(rows: List[Dict[str, Any]], metric: str, mode: str) -> List[Dict[str, Any]]:
    reverse = mode == "maximize"

    def score(row: Dict[str, Any]) -> float:
        value = metric_value(row, metric)
        if value is None:
            return float("-inf") if reverse else float("inf")
        return value

    return sorted(rows, key=score, reverse=reverse)


def run_index_by_name(rung: RungConfig) -> Dict[str, int]:
    mapping = {}  # type: Dict[str, int]
    for index, run in enumerate(load_sweep_runs(rung.sweep)):
        name = safe_name(str(run["name"]))
        if name in mapping:
            raise ValueError(f"Duplicate safe run name {name!r} in {rung.sweep}")
        mapping[name] = index
    return mapping


def build_sbatch_command(rung: RungConfig, indices: List[int], account: Optional[str] = None) -> List[str]:
    variables = default_vars()
    export_items = {
        "P_EDR_ROOT": variables["P_EDR_ROOT"],
        "ORANGE_EXPORT_ROOT": variables["ORANGE_EXPORT_ROOT"],
        "MELUXINA_PIDSMAKER_SWEEP": str(rung.sweep),
        "MELUXINA_PIDSMAKER_TAG": rung.tag,
        "MELUXINA_PIDSMAKER_PHASE": PID_PHASE,
        "MELUXINA_PIDSMAKER_RESULTS_DIR": str(rung.results_dir),
    }
    export_items.update({str(key): str(value) for key, value in rung.export_env.items()})
    # Do not inherit the controller job's Lmod environment into nested GPU jobs.
    # The array script is a login shell and loads its own modules.
    export_arg = ",".join(f"{key}={value}" for key, value in export_items.items())

    command = [
        "sbatch",
        f"--chdir={variables['P_EDR_ROOT']}",
        f"--array={array_spec(indices, rung.array_concurrency)}",
        f"--job-name={rung.job_name}",
    ]
    sbatch_key_map = {
        "account": "--account",
        "partition": "--partition",
        "qos": "--qos",
        "nodes": "--nodes",
        "mem": "--mem",
        "time": "--time",
        "constraint": "--constraint",
        "gres": "--gres",
        "cpus_per_task": "--cpus-per-task",
        "output": "--output",
        "error": "--error",
    }
    for key, flag in sbatch_key_map.items():
        value = account if key == "account" and account else rung.sbatch_options.get(key)
        if value not in (None, ""):
            command.append(f"{flag}={value}")
    for raw_arg in as_list(rung.sbatch_options.get("extra_args")):
        command.append(str(raw_arg))
    command.extend([f"--export={export_arg}", str(rung.sbatch_script)])
    return command


def submit_names(
    rung: RungConfig,
    names: List[str],
    dry_run: bool,
    account: Optional[str] = None,
) -> Tuple[Optional[str], List[int], List[str]]:
    if not names:
        return None, [], []
    mapping = run_index_by_name(rung)
    missing = [name for name in names if name not in mapping]
    if missing:
        raise ValueError(f"Promoted run(s) missing from {rung.sweep}: {missing}")
    indices = [mapping[name] for name in names]
    command = build_sbatch_command(rung, indices, account=account)
    account_text = f" account={account}" if account else ""
    print(f"submit rung={rung.name}{account_text} names={len(names)} indices={indices}", flush=True)
    print("$ " + shell_join(command), flush=True)
    if dry_run:
        return None, indices, command
    completed = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    output = (completed.stdout or "") + (completed.stderr or "")
    print(output.strip(), flush=True)
    if completed.returncode != 0:
        if "AssocMaxSubmitJobLimit" in output or "submit limit" in output.lower():
            raise SubmitLimitReached(output.strip())
        raise subprocess.CalledProcessError(completed.returncode, command, completed.stdout, completed.stderr)
    match = re.search(r"Submitted batch job\s+(\d+)", output)
    if not match:
        raise RuntimeError(f"Could not parse sbatch job id from output: {output!r}")
    return match.group(1), indices, command


def record_submission(
    state: Dict[str, Any],
    rung: RungConfig,
    names: List[str],
    job_id: Optional[str],
    indices: List[int],
    command: List[str],
    account: Optional[str],
) -> None:
    rung_state = state["rungs"][rung.name]
    rung_state["submitted"] = set_union_preserve_order(list(rung_state.get("submitted") or []), names)
    rung_state["submissions"].append(
        {"time": utc_now(), "job_id": job_id, "account": account, "names": names, "indices": indices, "command": command}
    )


def record_existing_results(state: Dict[str, Any], rung: RungConfig, names: List[str]) -> None:
    if not names:
        return
    mapping = run_index_by_name(rung)
    indices = [mapping[name] for name in names if name in mapping]
    rung_state = state["rungs"][rung.name]
    rung_state["submitted"] = set_union_preserve_order(list(rung_state.get("submitted") or []), names)
    rung_state["submissions"].append(
        {"time": utc_now(), "job_id": "existing_result", "names": names, "indices": indices, "command": ["existing_result"]}
    )


def write_status(config: AshaConfig, state: Dict[str, Any]) -> Dict[str, Any]:
    rung_statuses = []  # type: List[Dict[str, Any]]
    status = {
        "name": config.name,
        "time": utc_now(),
        "metric": config.metric,
        "mode": config.mode,
        "promotion_policy": config.promotion_policy,
        "reduction_factor": config.reduction_factor,
        "rungs": rung_statuses,
    }
    for rung in config.rungs:
        rung_state = state["rungs"][rung.name]
        planned = list(rung_state.get("planned") or [])
        submitted = list(rung_state.get("submitted") or [])
        promoted = list(rung_state.get("promoted") or [])
        cancelled = list(rung_state.get("cancelled") or [])
        rows = load_result_rows(rung)
        completed_names = [name for name in submitted if name in rows]
        valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
        ranked = rank_rows(valid_rows, config.metric, config.mode)
        rung_statuses.append(
            {
                "name": rung.name,
                "sweep": str(rung.sweep),
                "results_dir": str(rung.results_dir),
                "planned": len(planned),
                "submitted": len(submitted),
                "completed": len(completed_names),
                "valid": len(valid_rows),
                "promoted": len(promoted),
                "cancelled": len(cancelled),
                "best": ranked[0].get("name") if ranked else None,
                "best_metric": metric_value(ranked[0], config.metric) if ranked else None,
            }
        )
    if config.final:
        final_state = state.get("final") or {}
        final_rows = load_result_rows(config.final.rung)
        final_planned = list(final_state.get("planned") or [])
        final_submitted = list(final_state.get("submitted") or [])
        final_completed = [name for name in final_submitted if name in final_rows]
        final_valid_rows = [
            final_rows[name]
            for name in final_completed
            if int(final_rows[name].get("exit_code", 1) or 0) == 0
            and metric_value(final_rows[name], config.metric) is not None
        ]
        final_ranked = rank_rows(final_valid_rows, config.metric, config.mode)
        status["final"] = {
            "name": config.final.rung.name,
            "source_rung": config.final.source_rung,
            "top_k": config.final.top_k,
            "sweep": str(config.final.rung.sweep),
            "results_dir": str(config.final.rung.results_dir),
            "planned": len(final_planned),
            "submitted": len(final_submitted),
            "completed": len(final_completed),
            "valid": len(final_valid_rows),
            "best": final_ranked[0].get("name") if final_ranked else None,
            "best_metric": metric_value(final_ranked[0], config.metric) if final_ranked else None,
        }
    config.results_root.mkdir(parents=True, exist_ok=True)
    (config.results_root / "asha_status.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return status


def print_status(status: Dict[str, Any]) -> None:
    print(
        "ASHA status {} metric={} mode={} promotion_policy={}".format(
            status["name"], status["metric"], status["mode"], status["promotion_policy"]
        ),
        flush=True,
    )
    for rung in status["rungs"]:
        best_text = ""
        if rung.get("best") is not None:
            best_text = f" best={rung['best']} {status['metric']}={rung['best_metric']}"
        print(
            "  {name}: planned={planned} submitted={submitted} completed={completed} valid={valid} promoted={promoted} cancelled={cancelled}{best_text}".format(
                best_text=best_text, **rung
            ),
            flush=True,
        )
    if status.get("final"):
        final = status["final"]
        best_text = ""
        if final.get("best") is not None:
            best_text = f" best={final['best']} {status['metric']}={final['best_metric']}"
        print(
            "  final {name}: source={source_rung} top_k={top_k} planned={planned} submitted={submitted} completed={completed} valid={valid}{best_text}".format(
                best_text=best_text, **final
            ),
            flush=True,
        )


def initial_trial_names(config: AshaConfig) -> List[str]:
    first_rung_names = list(run_index_by_name(config.rungs[0]).keys())
    if config.start_trials is None:
        return first_rung_names
    missing = [name for name in config.start_trials if name not in first_rung_names]
    if missing:
        raise ValueError(f"start_trials missing from first rung sweep: {missing}")
    keep = set(config.start_trials)
    return [name for name in first_rung_names if name in keep]


@contextmanager
def submission_lock(config: AshaConfig):
    lock_path = config.results_root.parent / ".asha_submit.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def active_account_task_count(account: str) -> int:
    command = ["squeue", "-h", "-r", "-u", os.environ.get("USER", ""), "-A", account, "-o", "%i"]
    completed = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if completed.returncode != 0:
        output = (completed.stdout or "") + (completed.stderr or "")
        raise RuntimeError(f"Could not query active Slurm jobs for account {account}: {output.strip()}")
    return sum(1 for line in (completed.stdout or "").splitlines() if line.strip())


def account_capacity(account: str, max_submit_jobs: int) -> int:
    active = active_account_task_count(account)
    capacity = max(0, max_submit_jobs - active)
    print(f"account_capacity account={account} active={active} limit={max_submit_jobs} available={capacity}", flush=True)
    return capacity


def planned_names(state: Dict[str, Any], rung: RungConfig) -> List[str]:
    rung_state = state["rungs"][rung.name]
    planned = list(rung_state.get("planned") or [])
    if planned:
        return planned
    return list(rung_state.get("submitted") or [])


def set_planned(state: Dict[str, Any], rung: RungConfig, names: List[str]) -> bool:
    rung_state = state["rungs"][rung.name]
    previous = list(rung_state.get("planned") or [])
    updated = set_union_preserve_order(previous, names)
    if updated == previous:
        return False
    rung_state["planned"] = updated
    return True


def submit_accounts(rung: RungConfig) -> List[Optional[str]]:
    accounts = list(rung.submit_accounts)
    if not accounts and rung.sbatch_options.get("account"):
        accounts = [str(rung.sbatch_options["account"])]
    if not accounts:
        return [None]
    typed_accounts = []  # type: List[Optional[str]]
    typed_accounts.extend(accounts)
    return typed_accounts


def submit_pending_planned(config: AshaConfig, state: Dict[str, Any], rung: RungConfig, dry_run: bool) -> bool:
    rung_state = state["rungs"][rung.name]
    submitted = set(rung_state.get("submitted") or [])
    pending = [name for name in planned_names(state, rung) if name not in submitted]
    if not pending:
        return False

    changed = False
    rows = load_result_rows(rung)
    reusable = [name for name in pending if name in rows and result_is_reusable(config, rung, rows[name])]
    if reusable:
        print(f"reuse rung={rung.name} existing_results={len(reusable)} names={reusable}", flush=True)
        record_existing_results(state, rung, reusable)
        pending = [name for name in pending if name not in set(reusable)]
        changed = True
    if not pending:
        return changed

    for account in submit_accounts(rung):
        if not pending:
            break
        limit = rung.submit_chunk_size or len(pending)
        if account and rung.max_submit_jobs_per_account:
            capacity = account_capacity(account, rung.max_submit_jobs_per_account)
            if capacity <= 0:
                continue
            limit = min(limit, capacity)
        if limit <= 0:
            continue
        chunk = pending[:limit]
        try:
            job_id, indices, command = submit_names(rung, chunk, dry_run=dry_run, account=account)
        except SubmitLimitReached as exc:
            print(f"submit limit reached rung={rung.name} account={account}: {exc}", flush=True)
            continue
        record_submission(state, rung, chunk, job_id, indices, command, account)
        pending = pending[len(chunk) :]
        changed = True
    return changed


def record_final_submission(
    state: Dict[str, Any],
    names: List[str],
    job_id: Optional[str],
    indices: List[int],
    command: List[str],
    account: Optional[str],
) -> None:
    final_state = state["final"]
    final_state["submitted"] = set_union_preserve_order(list(final_state.get("submitted") or []), names)
    final_state["submissions"].append(
        {"time": utc_now(), "job_id": job_id, "account": account, "names": names, "indices": indices, "command": command}
    )


def record_existing_final_results(state: Dict[str, Any], final: FinalConfig, names: List[str]) -> None:
    if not names:
        return
    mapping = run_index_by_name(final.rung)
    indices = [mapping[name] for name in names if name in mapping]
    final_state = state["final"]
    final_state["submitted"] = set_union_preserve_order(list(final_state.get("submitted") or []), names)
    final_state["submissions"].append(
        {"time": utc_now(), "job_id": "existing_result", "names": names, "indices": indices, "command": ["existing_result"]}
    )


def final_result_is_complete(row: Dict[str, Any], metric: str) -> bool:
    if int(row.get("exit_code", 1) or 0) != 0:
        return False
    if bool(row.get("oom")):
        return False
    return metric_value(row, metric) is not None


def submit_pending_final(config: AshaConfig, state: Dict[str, Any], dry_run: bool) -> bool:
    if not config.final:
        return False
    final_state = state.get("final") or {}
    submitted = set(final_state.get("submitted") or [])
    pending = [name for name in list(final_state.get("planned") or []) if name not in submitted]
    if not pending:
        return False

    changed = False
    rows = load_result_rows(config.final.rung)
    reusable = [name for name in pending if name in rows and final_result_is_complete(rows[name], config.metric)]
    if reusable:
        print(f"reuse final={config.final.rung.name} existing_results={len(reusable)} names={reusable}", flush=True)
        record_existing_final_results(state, config.final, reusable)
        pending = [name for name in pending if name not in set(reusable)]
        changed = True
    if not pending:
        return changed

    for account in submit_accounts(config.final.rung):
        if not pending:
            break
        limit = config.final.rung.submit_chunk_size or len(pending)
        if account and config.final.rung.max_submit_jobs_per_account:
            capacity = account_capacity(account, config.final.rung.max_submit_jobs_per_account)
            if capacity <= 0:
                continue
            limit = min(limit, capacity)
        if limit <= 0:
            continue
        chunk = pending[:limit]
        try:
            job_id, indices, command = submit_names(config.final.rung, chunk, dry_run=dry_run, account=account)
        except SubmitLimitReached as exc:
            print(f"submit limit reached final={config.final.rung.name} account={account}: {exc}", flush=True)
            continue
        record_final_submission(state, chunk, job_id, indices, command, account)
        pending = pending[len(chunk) :]
        changed = True
    return changed


def max_promotion_count(submitted_count: int, reduction_factor: int) -> int:
    if submitted_count <= 0:
        return 0
    return max(1, submitted_count // reduction_factor)


def promotion_target(completed_count: int, submitted_count: int, reduction_factor: int) -> int:
    if submitted_count <= 0:
        return 0
    target = completed_count // reduction_factor
    if completed_count == submitted_count and completed_count > 0 and target == 0:
        target = 1
    return min(target, max_promotion_count(submitted_count, reduction_factor))


def sync_promotion_target(ranked_count: int, submitted_count: int, reduction_factor: int) -> int:
    return min(ranked_count, max_promotion_count(submitted_count, reduction_factor))


def submitted_index_by_name(rung_state: Dict[str, Any]) -> Dict[str, Tuple[str, int]]:
    mapping = {}  # type: Dict[str, Tuple[str, int]]
    for submission in list(rung_state.get("submissions") or []):
        names = list(submission.get("names") or [])
        indices = list(submission.get("indices") or [])
        job_id = submission.get("job_id")
        if not job_id or job_id == "existing_result":
            continue
        for name, index in zip(names, indices):
            mapping[str(name)] = (str(job_id), int(index))
    return mapping


def slurm_state(job_token: str) -> Optional[str]:
    completed = subprocess.run(
        ["squeue", "-h", "-j", job_token, "-o", "%T"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    states = [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]
    return states[0] if states else None


def active_submitted_names(rung_state: Dict[str, Any], names: List[str]) -> List[str]:
    index_by_name = submitted_index_by_name(rung_state)
    active = []
    for name in names:
        job_info = index_by_name.get(name)
        if not job_info:
            continue
        job_id, index = job_info
        state = slurm_state(f"{job_id}_{index}")
        if state in ACTIVE_SLURM_STATES:
            active.append(name)
    return active


def cancel_pending_tokens(job_tokens: List[str], dry_run: bool) -> List[str]:
    cancelled = []
    for token in job_tokens:
        if dry_run:
            print(f"would check/cancel pending {token}", flush=True)
            cancelled.append(token)
            continue
        state = slurm_state(token)
        if state != "PENDING":
            continue
        print(f"cancel pending {token}", flush=True)
        subprocess.run(["scancel", token], check=False)
        cancelled.append(token)
    return cancelled


def maybe_cancel_remaining_async(
    config: AshaConfig,
    state: Dict[str, Any],
    rung: RungConfig,
    rows: Dict[str, Dict[str, Any]],
    dry_run: bool,
) -> bool:
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
        tokens.append(f"{job_id}_{index}")
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


def maybe_submit_initial(config: AshaConfig, state: Dict[str, Any], dry_run: bool) -> bool:
    first = config.rungs[0]
    first_state = state["rungs"][first.name]
    if first_state.get("planned"):
        return False
    names = initial_trial_names(config)
    if not names:
        raise ValueError("No initial trials selected")
    print(f"plan initial rung={first.name} names={len(names)}", flush=True)
    if not dry_run:
        first_state["planned"] = names
    else:
        first_state["planned"] = names
    return True


def maybe_promote(config: AshaConfig, state: Dict[str, Any], dry_run: bool) -> bool:
    changed = False
    for rung_index, rung in enumerate(config.rungs[:-1]):
        next_rung = config.rungs[rung_index + 1]
        rung_state = state["rungs"][rung.name]
        next_state = state["rungs"][next_rung.name]
        planned = planned_names(state, rung)
        if not planned:
            continue
        submitted = list(rung_state.get("submitted") or [])
        if any(name not in set(submitted) for name in planned):
            continue
        rows = load_result_rows(rung)
        completed_names = [name for name in planned if name in rows]
        completed_count = len(completed_names)
        if config.promotion_policy == "sync":
            if completed_count < len(planned):
                continue
            require_checkpoint = rung_requires_resume(next_rung)
            valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
            ranked = rank_rows(valid_rows, config.metric, config.mode)
            incomplete = [
                name
                for name in planned
                if name not in rows
                or not result_is_promotable(rows[name], config.metric)
            ]
            active_incomplete = active_submitted_names(rung_state, incomplete)
            if active_incomplete:
                print(
                    f"wait promotion rung={rung.name} active_incomplete={len(active_incomplete)}",
                    flush=True,
                )
                continue
            target = sync_promotion_target(len(ranked), len(planned), config.reduction_factor)
            if require_checkpoint and target > 0:
                candidate_names = [str(row["name"]) for row in ranked[:target]]
                missing_candidate_checkpoints = [
                    name for name in candidate_names if name in rows and not result_has_checkpoint(rows[name])
                ]
                if missing_candidate_checkpoints:
                    active_checkpoint_jobs = active_submitted_names(rung_state, missing_candidate_checkpoints)
                    print(
                        "wait promotion rung={} missing_candidate_checkpoints={} active_checkpoint_jobs={}".format(
                            rung.name, len(missing_candidate_checkpoints), len(active_checkpoint_jobs)
                        ),
                        flush=True,
                    )
                    continue
        else:
            target = promotion_target(completed_count, len(planned), config.reduction_factor)
            require_checkpoint = rung_requires_resume(next_rung)
            valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
            ranked = rank_rows(valid_rows, config.metric, config.mode)
        already_promoted = list(rung_state.get("promoted") or [])
        remaining_slots = target - len(already_promoted)
        if config.promotion_policy == "async" and len(already_promoted) >= max_promotion_count(len(planned), config.reduction_factor):
            changed = maybe_cancel_remaining_async(config, state, rung, rows, dry_run) or changed
        if remaining_slots <= 0:
            continue
        already_promoted_set = set(already_promoted)
        next_planned_set = set(next_state.get("planned") or [])
        candidates = [
            str(row["name"])
            for row in ranked
            if str(row.get("name")) not in already_promoted_set
            and str(row.get("name")) not in next_planned_set
        ]
        promote = candidates[:remaining_slots]
        if not promote:
            continue

        print(f"plan promotion {rung.name}->{next_rung.name} names={len(promote)}", flush=True)
        if not dry_run:
            rung_state["promoted"] = set_union_preserve_order(already_promoted, promote)
            set_planned(state, next_rung, promote)
            if config.promotion_policy == "async" and len(rung_state["promoted"]) >= max_promotion_count(len(planned), config.reduction_factor):
                changed = maybe_cancel_remaining_async(config, state, rung, rows, dry_run) or changed
        else:
            set_planned(state, next_rung, promote)
        changed = True
    return changed


def maybe_plan_final(config: AshaConfig, state: Dict[str, Any], dry_run: bool) -> bool:
    if not config.final:
        return False
    final_state = state.get("final") or {}
    if final_state.get("planned"):
        return False

    source_rung = None
    for rung in config.rungs:
        if rung.name == config.final.source_rung:
            source_rung = rung
            break
    if source_rung is None:
        raise ValueError(f"Unknown final source rung: {config.final.source_rung}")

    source_state = state["rungs"][source_rung.name]
    source_planned = planned_names(state, source_rung)
    if not source_planned:
        return False
    source_submitted = set(source_state.get("submitted") or [])
    if any(name not in source_submitted for name in source_planned):
        return False

    rows = load_result_rows(source_rung)
    completed_names = [name for name in source_planned if name in rows]
    if len(completed_names) < len(source_planned):
        return False

    valid_rows = [rows[name] for name in completed_names if result_is_promotable(rows[name], config.metric)]
    ranked = rank_rows(valid_rows, config.metric, config.mode)
    selected = [str(row["name"]) for row in ranked[: config.final.top_k]]
    if not selected:
        print(f"final source={source_rung.name} has no valid rows; cannot plan final", flush=True)
        return False

    print(
        "plan final source={} final={} top_k={} selected={}".format(
            source_rung.name, config.final.rung.name, config.final.top_k, selected
        ),
        flush=True,
    )
    final_state = state["final"]
    final_state["source_rung"] = config.final.source_rung
    final_state["top_k"] = config.final.top_k
    final_state["planned"] = selected
    final_state["submitted"] = []
    final_state["cancelled"] = []
    final_state["submissions"] = []
    return True


def all_done(config: AshaConfig, state: Dict[str, Any]) -> bool:
    if config.final:
        final_state = state.get("final") or {}
        final_planned = list(final_state.get("planned") or [])
        if not final_planned:
            return False
        final_submitted = set(final_state.get("submitted") or [])
        if any(name not in final_submitted for name in final_planned):
            return False
        final_rows = load_result_rows(config.final.rung)
        return all(name in final_rows for name in final_planned)

    final_rung = config.rungs[-1]
    final_planned = planned_names(state, final_rung)
    if not final_planned:
        return False
    final_submitted = set(state["rungs"][final_rung.name].get("submitted") or [])
    if any(name not in final_submitted for name in final_planned):
        return False
    rows = load_result_rows(final_rung)
    return all(name in rows for name in final_planned)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print sbatch commands without submitting or updating state")
    parser.add_argument("--poll-seconds", type=int, help="Override config poll interval")
    parser.add_argument("--max-loops", type=int, help="Stop after this many polling iterations")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    poll_seconds = args.poll_seconds or config.poll_seconds
    state = load_state(config)

    loops = 0
    while True:
        loops += 1
        with submission_lock(config):
            changed = maybe_submit_initial(config, state, dry_run=args.dry_run)
            for rung in config.rungs:
                changed = submit_pending_planned(config, state, rung, dry_run=args.dry_run) or changed
            promoted = maybe_promote(config, state, dry_run=args.dry_run)
            changed = promoted or changed
            if promoted:
                for rung in config.rungs:
                    changed = submit_pending_planned(config, state, rung, dry_run=args.dry_run) or changed
            final_planned = maybe_plan_final(config, state, dry_run=args.dry_run)
            changed = final_planned or changed
            changed = submit_pending_final(config, state, dry_run=args.dry_run) or changed
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
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
