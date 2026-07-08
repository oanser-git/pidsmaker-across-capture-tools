#!/usr/bin/env python3
"""Run one HPO config inside Apptainer."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml


CHECKPOINT_MOUNT = "/home/checkpoints"
DEFAULT_COMPLETION_METRIC = "adp_score"

METRIC_KEYS = (
    "adp_score",
    "precision",
    "recall",
    "f1",
    "tp",
    "fp",
    "tn",
    "fn",
    "auc",
    "ap",
    "percent_detected_attacks",
    "discrimination",
)
METRIC_RE = re.compile(
    r"['\"]?(?P<key>" + "|".join(METRIC_KEYS) + r")['\"]?\s*[:=]\s*(?:np\.\w+\()?(?P<value>[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)",
    re.IGNORECASE,
)
VAL_RE = re.compile(r"\[@epoch(?P<epoch>\d+)\].*Val.*Loss:\s*(?P<loss>[-+]?\d+(?:\.\d+)?)", re.IGNORECASE)
TEST_RE = re.compile(r"\[@epoch(?P<epoch>\d+)\].*Test.*Loss:\s*(?P<loss>[-+]?\d+(?:\.\d+)?)", re.IGNORECASE)


def workspace_root() -> Path:
    return Path(os.environ.get("P_EDR_ROOT", Path(__file__).resolve().parents[2])).resolve()


def default_vars() -> Dict[str, str]:
    root = workspace_root()
    export_root = Path(os.environ.get("ORANGE_EXPORT_ROOT", root / "capture_export" / "pidsmaker_export")).resolve()
    return {
        "P_EDR_ROOT": str(root),
        "ORANGE_EXPORT_ROOT": str(export_root),
    }


def safe_name(value: str, max_length: int = 80) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_.-")[:max_length]


def expand(value: Any, variables: Dict[str, str]) -> Any:
    if isinstance(value, str):
        for key, replacement in variables.items():
            value = value.replace("${" + key + "}", replacement)
    elif isinstance(value, list):
        value = [expand(item, variables) for item in value]
    elif isinstance(value, dict):
        value = {key: expand(item, variables) for key, item in value.items()}
    return value


def load_sweep(path: Path, tag: Optional[str], apply_env: bool = True) -> Dict[str, Any]:
    variables = default_vars()
    sweep = expand(yaml.safe_load(path.read_text()) or {}, variables)
    if apply_env and os.environ.get("HPO_SWEEP_NAME"):
        sweep["name"] = safe_name(os.environ["HPO_SWEEP_NAME"])
    if apply_env and os.environ.get("HPO_DATASET"):
        sweep["dataset"] = os.environ["HPO_DATASET"]
    if apply_env and os.environ.get("HPO_METHOD"):
        sweep["method"] = os.environ["HPO_METHOD"]
    if tag:
        sweep["name"] = safe_name(f"{sweep['name']}_{safe_name(tag)}")
        results_dir = Path(str(sweep["results_dir"]))
        sweep["results_dir"] = str(results_dir.with_name(f"{results_dir.name}_{safe_name(tag)}"))
        if sweep.get("archive_dir"):
            archive_dir = Path(str(sweep["archive_dir"]))
            sweep["archive_dir"] = str(archive_dir.with_name(f"{archive_dir.name}_{safe_name(tag)}"))
    if apply_env and os.environ.get("HPO_RESULTS_DIR"):
        sweep["results_dir"] = os.environ["HPO_RESULTS_DIR"]
    if apply_env and os.environ.get("HPO_ARTIFACT_ROOT"):
        sweep["artifact_root"] = os.environ["HPO_ARTIFACT_ROOT"]
    if apply_env and os.environ.get("HPO_ARCHIVE_ROOT"):
        sweep["archive_dir"] = os.environ["HPO_ARCHIVE_ROOT"]
    if "run_templates" in sweep:
        runs = []
        for template in sweep["run_templates"]:
            base_overrides = dict(template.get("overrides") or {})
            for seed in sweep.get("seeds", [None]):
                overrides = dict(base_overrides)
                suffix = ""
                if seed is not None:
                    suffix = f"_seed{seed}"
                    overrides.setdefault("training.seed", seed)
                    overrides.setdefault("featurization.seed", seed)
                runs.append({"name": f"{template['name']}{suffix}", "overrides": overrides})
        sweep["runs"] = runs
    return sweep


def artifact_name(sweep: Dict[str, Any], run: Dict[str, Any], phase: str) -> str:
    run_name = safe_name(str(run["name"]))
    suffix = "_final_test" if phase == "final_test" else ""
    return safe_name(
        str(run.get("artifact") or f"{sweep['name']}_{run_name}{suffix}"),
        max_length=180,
    )


def find_run_by_name(sweep: Dict[str, Any], run_name: str) -> Dict[str, Any]:
    for run in sweep.get("runs") or []:
        if safe_name(str(run["name"])) == run_name:
            return run
    raise SystemExit(f"Unknown resume run {run_name} in previous sweep")


def resume_artifact_for_run(run_name: str) -> Optional[str]:
    if os.environ.get("HPO_RESUME_FROM_ARTIFACT"):
        return safe_name(os.environ["HPO_RESUME_FROM_ARTIFACT"], max_length=180)

    previous_sweep = os.environ.get("HPO_RESUME_FROM_SWEEP")
    if not previous_sweep:
        return None

    previous_tag = os.environ.get("HPO_RESUME_FROM_TAG")
    previous_phase = os.environ.get("HPO_RESUME_FROM_PHASE", "hpo")
    sweep = load_sweep(Path(previous_sweep).resolve(), previous_tag, apply_env=False)
    run = find_run_by_name(sweep, run_name)
    return artifact_name(sweep, run, previous_phase)


def checkpoint_is_ready(path: Path) -> bool:
    return (path / "state_dict.pkl").exists() and (path / "metadata.json").exists()


def checkpoint_reached_target(path: Path) -> bool:
    if not checkpoint_is_ready(path):
        return False
    try:
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    next_epoch = metadata.get("next_epoch")
    target_epochs = metadata.get("target_epochs")
    if next_epoch is None or target_epochs is None:
        return False
    return int(next_epoch) >= int(target_epochs)


def cli_value(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


def completion_metric(sweep: Dict[str, Any]) -> str:
    return str(
        os.environ.get("HPO_COMPLETION_METRIC")
        or sweep.get("completion_metric")
        or sweep.get("selection_metric")
        or DEFAULT_COMPLETION_METRIC
    )


def select_run(sweep: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    runs = list(sweep["runs"])
    if args.run_name:
        for run in runs:
            if run["name"] == args.run_name:
                return run
        raise SystemExit(f"Unknown run: {args.run_name}")
    index = args.index if args.index is not None else int(os.environ["SLURM_ARRAY_TASK_ID"])
    if index < 0 or index >= len(runs):
        raise SystemExit(f"Run index out of range: {index}")
    return runs[index]


def build_command(sweep: Dict[str, Any], run: Dict[str, Any], args: argparse.Namespace) -> Tuple[List[str], Dict[str, Any]]:
    run_name = safe_name(str(run["name"]))
    phase = args.phase or ("final_test" if args.run_test else "hpo")
    artifact = artifact_name(sweep, run, phase)
    repo_root = Path(str(sweep["repo_root"])).resolve()
    artifact_root = Path(str(sweep["artifact_root"])).resolve()
    artifact_dir = artifact_root / artifact
    archive_dir = Path(str(sweep["archive_dir"])).resolve() if sweep.get("archive_dir") else None
    checkpoint_root = None
    if os.environ.get("HPO_CHECKPOINT_ROOT") or sweep.get("checkpoint_root"):
        checkpoint_root = Path(str(os.environ.get("HPO_CHECKPOINT_ROOT") or sweep.get("checkpoint_root"))).resolve()
    variables = default_vars()
    export_root = Path(variables["ORANGE_EXPORT_ROOT"])
    image_env = os.environ.get("HPO_IMAGE")
    if not image_env:
        raise SystemExit("HPO_IMAGE must point to the PIDSMaker Apptainer image")
    image = Path(image_env).resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    if checkpoint_root:
        checkpoint_root.mkdir(parents=True, exist_ok=True)
    apptainer_home = artifact_root / "apptainer_home"
    apptainer_home.mkdir(parents=True, exist_ok=True)
    (artifact_root / "nltk_data").mkdir(parents=True, exist_ok=True)
    (artifact_root / "matplotlib").mkdir(parents=True, exist_ok=True)
    container_home = os.environ.get("HPO_CONTAINER_HOME", "/home/runtime")

    inner = [
        "python",
        "-u",
        "-m",
        "pidsmaker.main",
        str(sweep["method"]),
        str(sweep["dataset"]),
        "--artifact_dir",
        f"/home/artifacts/{artifact}",
        f"--training.num_epochs={int(sweep.get('epochs', 12))}",
    ]
    if os.environ.get("HPO_CPU") == "1":
        inner.append("--cpu")
    for key, value in dict(run.get("overrides") or {}).items():
        inner.append(f"--{key}={cli_value(value)}")

    binds = [
        f"{repo_root}:/home/pids",
        f"{artifact_root}:/home/artifacts",
        f"{export_root}:{export_root}:ro",
    ]
    container_env = {
        "P_EDR_ROOT": variables["P_EDR_ROOT"],
        "ORANGE_EXPORT_ROOT": variables["ORANGE_EXPORT_ROOT"],
    }
    checkpoint_out_host = None
    resume_from_host = None
    if checkpoint_root:
        binds.append(f"{checkpoint_root}:{CHECKPOINT_MOUNT}")
        checkpoint_out_host = checkpoint_root / artifact / "latest"
        container_env["PIDSMAKER_CHECKPOINT_OUT"] = f"{CHECKPOINT_MOUNT}/{artifact}/latest"
        resume_artifact = resume_artifact_for_run(run_name)
        forced_resume = bool(os.environ.get("HPO_RESUME_FROM_ARTIFACT"))
        if forced_resume and resume_artifact:
            resume_from_host = checkpoint_root / resume_artifact / "latest"
            if not checkpoint_is_ready(resume_from_host):
                raise FileNotFoundError(f"Missing resume checkpoint: {resume_from_host}")
            container_env["PIDSMAKER_RESUME_FROM"] = f"{CHECKPOINT_MOUNT}/{resume_artifact}/latest"
        elif checkpoint_is_ready(checkpoint_out_host):
            resume_from_host = checkpoint_out_host
            container_env["PIDSMAKER_RESUME_FROM"] = f"{CHECKPOINT_MOUNT}/{artifact}/latest"
            container_env["PIDSMAKER_RESTORE_OPTIMIZER"] = "1"
        elif resume_artifact:
            resume_from_host = checkpoint_root / resume_artifact / "latest"
            if not checkpoint_is_ready(resume_from_host):
                raise FileNotFoundError(f"Missing resume checkpoint: {resume_from_host}")
            container_env["PIDSMAKER_RESUME_FROM"] = f"{CHECKPOINT_MOUNT}/{resume_artifact}/latest"
    elif os.environ.get("HPO_RESUME_FROM_SWEEP") or os.environ.get("HPO_RESUME_FROM_ARTIFACT"):
        raise SystemExit("HPO_CHECKPOINT_ROOT is required when resume is requested")

    command = [
        "apptainer",
        "exec",
        "--no-mount",
        "home",
        "--home",
        f"{apptainer_home}:{container_home}",
    ]
    for bind in binds:
        command.extend(["--bind", bind])
    command += ["--pwd", "/home/pids", str(image)] + inner
    if os.environ.get("HPO_CPU") != "1" or os.environ.get("HPO_FORCE_NV") == "1":
        command.insert(2, "--nv")
    meta = {
        "name": run_name,
        "phase": phase,
        "artifact": artifact,
        "artifact_dir": str(artifact_dir),
        "artifact_archive": str(archive_dir / f"{phase}_{run_name}.tar") if archive_dir else None,
        "checkpoint_out": str(checkpoint_out_host) if checkpoint_out_host else None,
        "resume_from": str(resume_from_host) if resume_from_host else None,
        "container_env": container_env,
        "overrides": run.get("overrides") or {},
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
        "node": os.environ.get("SLURMD_NODENAME"),
    }
    return command, meta


def archive_artifact(meta: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    artifact_dir = Path(str(meta["artifact_dir"]))
    archive = meta.get("artifact_archive")
    if not archive or not artifact_dir.exists():
        return None, None

    archive_path = Path(str(archive))
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = archive_path.with_name(f".{archive_path.name}.tmp.{os.getpid()}")
    try:
        with tarfile.open(tmp_path, "w") as tar:
            tar.add(artifact_dir, arcname=artifact_dir.name)
        os.replace(tmp_path, archive_path)
        return str(archive_path), None
    except Exception as exc:  # noqa: BLE001 - keep metrics even if archival fails.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None, f"{type(exc).__name__}: {exc}"


def parse_metrics(text: str) -> Dict[str, Any]:
    result = {}
    vals = [(int(m.group("epoch")), float(m.group("loss"))) for m in VAL_RE.finditer(text)]
    tests = [(int(m.group("epoch")), float(m.group("loss"))) for m in TEST_RE.finditer(text)]
    if vals:
        epoch, loss = min(vals, key=lambda item: item[1])
        result.update({"best_epoch": epoch, "best_val_loss": loss})
    if tests:
        epoch, loss = tests[-1]
        result.update({"test_epoch": epoch, "test_loss": loss})
    for match in METRIC_RE.finditer(text):
        result[match.group("key").lower()] = float(match.group("value"))
    lowered = text.lower()
    result["oom"] = any(
        marker in lowered
        for marker in ("out of memory", "oom-kill", "oom killed", "oom_kill")
    )
    return result


def run_command(command: List[str], meta: Dict[str, Any], results_dir: Path, required_metric: str) -> int:
    print("$ " + " ".join(shlex.quote(part) for part in command), flush=True)
    started = time.time()
    env = os.environ.copy()
    env.setdefault("APPTAINERENV_NLTK_DATA", "/home/artifacts/nltk_data")
    env.setdefault("APPTAINERENV_MPLCONFIGDIR", "/home/artifacts/matplotlib")
    env.setdefault("APPTAINERENV_XDG_CACHE_HOME", "/home/artifacts/cache")
    for key, value in dict(meta.get("container_env") or {}).items():
        env[key] = str(value)
        env[f"APPTAINERENV_{key}"] = str(value)
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1, env=env)
    assert proc.stdout is not None
    lines = []
    graceful_finished = False
    for line in proc.stdout:
        print(line, end="", flush=True)
        lines.append(line)
        if "Run finished." in line:
            graceful_finished = True
            proc.terminate()
            break
    try:
        exit_code = proc.wait(timeout=30 if graceful_finished else None)
    except subprocess.TimeoutExpired:
        proc.kill()
        exit_code = proc.wait()
    if graceful_finished:
        exit_code = 0
    row = {
        **meta,
        "exit_code": exit_code,
        "duration_sec": round(time.time() - started, 1),
    }
    row.update(parse_metrics("".join(lines)))
    row["oom"] = bool(row.get("oom")) or exit_code == 137
    checkpoint_out = meta.get("checkpoint_out")
    if (
        row.get("phase") == "hpo"
        and exit_code != 0
        and not row["oom"]
        and row.get(required_metric) is not None
        and checkpoint_out
        and checkpoint_reached_target(Path(str(checkpoint_out)))
    ):
        row["exit_code_before_hpo_completion_override"] = exit_code
        row["hpo_completion_override"] = True
        exit_code = 0
        row["exit_code"] = 0
    artifact_archive, archive_error = archive_artifact(meta)
    if artifact_archive:
        row["artifact_archive"] = artifact_archive
    if archive_error:
        row["artifact_archive_error"] = archive_error
    results_dir.mkdir(parents=True, exist_ok=True)
    result_path = results_dir / f"{meta['phase']}_{meta['name']}.json"
    tmp_path = result_path.with_name(f".{result_path.name}.tmp.{os.getpid()}")
    tmp_path.write_text(json.dumps(row, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, result_path)
    return exit_code


def should_skip_existing(results_dir: Path, meta: Dict[str, Any], required_metric: str) -> bool:
    if os.environ.get("HPO_SKIP_EXISTING", "").lower() not in {"1", "true", "yes"}:
        return False
    result_path = results_dir / f"{meta['phase']}_{meta['name']}.json"
    if not result_path.exists():
        return False
    try:
        row = json.loads(result_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if row.get("exit_code") != 0:
        return False
    if meta.get("phase") == "hpo" and row.get(required_metric) is None:
        return False
    checkpoint_out = meta.get("checkpoint_out")
    if checkpoint_out and not checkpoint_is_ready(Path(str(checkpoint_out))):
        return False
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sweep", type=Path, required=True)
    parser.add_argument("--index", type=int)
    parser.add_argument("--run-name")
    parser.add_argument("--tag")
    parser.add_argument("--run-test", action="store_true")
    parser.add_argument("--phase", choices=["hpo", "final_test"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sweep = load_sweep(args.sweep.resolve(), args.tag)
    run = select_run(sweep, args)
    command, meta = build_command(sweep, run, args)
    results_dir = Path(str(sweep["results_dir"])).resolve()
    required_metric = completion_metric(sweep)
    if should_skip_existing(results_dir, meta, required_metric):
        print(f"Skipping completed run: {meta['name']}", flush=True)
        sys.exit(0)
    sys.exit(run_command(command, meta, results_dir, required_metric))


if __name__ == "__main__":
    main()
