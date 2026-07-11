#!/usr/bin/env python3
"""Run one PIDSMaker configuration inside Apptainer on MeluXina."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import shlex
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml


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
VAL_RE = re.compile(
    r"\[@epoch(?P<epoch>\d+)\].*Val.*Loss:\s*(?P<loss>[-+]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
TEST_RE = re.compile(
    r"\[@epoch(?P<epoch>\d+)\].*Test.*Loss:\s*(?P<loss>[-+]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def workspace_root() -> Path:
    return Path(os.environ.get("P_EDR_ROOT", Path(__file__).resolve().parents[2])).resolve()


def default_vars() -> Dict[str, str]:
    root = workspace_root()
    meluxina_dir = root / "meluxina"
    return {
        "P_EDR_ROOT": str(root),
        "MELUXINA_DIR": str(meluxina_dir),
        "ORANGE_EXPORT_ROOT": str(
            Path(
                os.environ.get(
                    "ORANGE_EXPORT_ROOT",
                    "/mnt/tier2/project/p201223/pidsmaker-across-capture-tools/capture_export/pidsmaker_export",
                )
            ).resolve()
        ),
    }


def safe_name(value: str, max_length: int = 160) -> str:
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


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise SystemExit("Sweep must be a YAML mapping: {}".format(path))
    return payload


def load_sweep(path: Path, tag: Optional[str]) -> Dict[str, Any]:
    variables = default_vars()
    sweep = expand(load_yaml(path), variables)
    sweep.setdefault("repo_root", "${P_EDR_ROOT}/external/PIDSMaker")
    sweep.setdefault("artifact_root", "${MELUXINA_DIR}/pidsmaker/artifacts")
    sweep.setdefault(
        "results_dir",
        "${{MELUXINA_DIR}}/pidsmaker/results/{}".format(safe_name(str(sweep.get("name", "run")))),
    )
    sweep = expand(sweep, variables)

    if tag:
        sweep["name"] = safe_name("{}_{}".format(sweep["name"], tag))
        results_dir = Path(str(sweep["results_dir"]))
        sweep["results_dir"] = str(results_dir.with_name("{}_{}".format(results_dir.name, safe_name(tag))))

    for required in ("name", "method", "dataset", "runs"):
        if not sweep.get(required):
            raise SystemExit("Sweep is missing required field: {}".format(required))
    return sweep


def cli_value(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


def select_run(sweep: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    runs = list(sweep["runs"])
    if args.run_name:
        for run in runs:
            if str(run["name"]) == args.run_name:
                return run
        raise SystemExit("Unknown run: {}".format(args.run_name))
    if args.index is not None:
        index = args.index
    else:
        index = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
    if index < 0 or index >= len(runs):
        raise SystemExit("Run index out of range: {}".format(index))
    return runs[index]


def build_command(sweep: Dict[str, Any], run: Dict[str, Any], args: argparse.Namespace) -> Tuple[List[str], Dict[str, Any]]:
    variables = default_vars()
    image_env = os.environ.get("MELUXINA_PIDSMAKER_IMAGE") or os.environ.get("HPO_IMAGE")
    if not image_env:
        raise SystemExit("Set MELUXINA_PIDSMAKER_IMAGE to the PIDSMaker Apptainer image")

    image = Path(image_env).resolve()
    repo_root = Path(str(sweep["repo_root"])).resolve()
    artifact_root = Path(str(os.environ.get("MELUXINA_PIDSMAKER_ARTIFACT_ROOT") or sweep["artifact_root"])).resolve()
    export_root = Path(str(run.get("export_root") or sweep.get("export_root") or variables["ORANGE_EXPORT_ROOT"])).resolve()
    phase = args.phase or "run"
    run_name = safe_name(str(run["name"]))
    artifact = safe_name("{}_{}_{}".format(sweep["name"], run_name, phase), max_length=200)

    artifact_root.mkdir(parents=True, exist_ok=True)
    task_cache_root = artifact_root / "_task_cache" / artifact
    nltk_data = task_cache_root / "nltk_data"
    matplotlib_config = task_cache_root / "matplotlib"
    xdg_cache = task_cache_root / "cache"
    apptainer_home = task_cache_root / "apptainer_home"
    nltk_data.mkdir(parents=True, exist_ok=True)
    matplotlib_config.mkdir(parents=True, exist_ok=True)
    xdg_cache.mkdir(parents=True, exist_ok=True)
    apptainer_home.mkdir(parents=True, exist_ok=True)

    checkpoint_dir = Path(str(sweep["checkpoint_dir"])).resolve() if sweep.get("checkpoint_dir") else None
    resume_checkpoint_dir = (
        Path(str(sweep["resume_checkpoint_dir"])).resolve() if sweep.get("resume_checkpoint_dir") else None
    )
    host_save_checkpoint = task_cache_root / "training_checkpoint.pt" if checkpoint_dir else None
    persist_checkpoint = checkpoint_dir / f"{run_name}.pt" if checkpoint_dir else None
    host_resume_checkpoint = None
    if resume_checkpoint_dir:
        source_checkpoint = resume_checkpoint_dir / f"{run_name}.pt"
        if not source_checkpoint.exists():
            raise SystemExit(f"Missing resume checkpoint for {run_name}: {source_checkpoint}")
        host_resume_checkpoint = task_cache_root / "resume_checkpoint.pt"
        shutil.copy2(source_checkpoint, host_resume_checkpoint)

    inner = [
        "python",
        "-u",
        "-m",
        "pidsmaker.main",
        str(sweep["method"]),
        str(sweep["dataset"]),
        "--artifact_dir",
        "/home/artifacts/{}".format(artifact),
        "--training.num_epochs={}".format(int(sweep.get("epochs", 12))),
    ]
    if os.environ.get("MELUXINA_PIDSMAKER_CPU") == "1":
        inner.append("--cpu")
    for key, value in dict(run.get("overrides") or {}).items():
        inner.append("--{}={}".format(key, cli_value(value)))
    for extra_arg in list(sweep.get("extra_args") or []):
        inner.append(str(extra_arg))
    for extra_arg in args.extra_arg or []:
        inner.append(str(extra_arg))

    command = [
        "apptainer",
        "exec",
        "--no-mount",
        "home",
        "--home",
        "{}:/home/runtime".format(apptainer_home),
        "--bind",
        "{}:/home/pids".format(repo_root),
        "--bind",
        "{}:/home/artifacts".format(artifact_root),
        "--bind",
        "{}:{}:ro".format(export_root, export_root),
        "--pwd",
        "/home/pids",
        str(image),
    ] + inner
    if os.environ.get("MELUXINA_PIDSMAKER_CPU") != "1" or os.environ.get("MELUXINA_PIDSMAKER_FORCE_NV") == "1":
        command.insert(2, "--nv")

    meta = {
        "name": run_name,
        "phase": phase,
        "artifact": artifact,
        "artifact_dir": str(artifact_root / artifact),
        "container_nltk_data": "/home/artifacts/_task_cache/{}/nltk_data".format(artifact),
        "container_matplotlib_config": "/home/artifacts/_task_cache/{}/matplotlib".format(artifact),
        "container_xdg_cache": "/home/artifacts/_task_cache/{}/cache".format(artifact),
        "container_save_checkpoint": "/home/artifacts/_task_cache/{}/training_checkpoint.pt".format(artifact)
        if host_save_checkpoint
        else None,
        "container_resume_checkpoint": "/home/artifacts/_task_cache/{}/resume_checkpoint.pt".format(artifact)
        if host_resume_checkpoint
        else None,
        "host_save_checkpoint": str(host_save_checkpoint) if host_save_checkpoint else None,
        "persist_checkpoint": str(persist_checkpoint) if persist_checkpoint else None,
        "host_resume_checkpoint": str(host_resume_checkpoint) if host_resume_checkpoint else None,
        "resume_checkpoint": str(resume_checkpoint_dir / f"{run_name}.pt") if resume_checkpoint_dir else None,
        "method": str(sweep["method"]),
        "dataset": str(sweep["dataset"]),
        "epochs": int(sweep.get("epochs", 12)),
        "orange_export_root": str(export_root),
        "export_variant": run.get("export_variant"),
        "export_window_size_seconds": run.get("export_window_size_seconds"),
        "overrides": run.get("overrides") or {},
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
        "node": os.environ.get("SLURMD_NODENAME"),
    }
    return command, meta


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
    result["oom"] = any(marker in lowered for marker in ("out of memory", "oom-kill", "oom killed", "oom_kill"))
    return result


def run_command(command: List[str], meta: Dict[str, Any], results_dir: Path) -> int:
    print("$ " + " ".join(shlex.quote(part) for part in command), flush=True)
    started = time.time()
    env = os.environ.copy()
    orange_export_root = str(meta.get("orange_export_root") or default_vars()["ORANGE_EXPORT_ROOT"])
    env["ORANGE_EXPORT_ROOT"] = orange_export_root
    env["APPTAINERENV_ORANGE_EXPORT_ROOT"] = orange_export_root
    env["APPTAINERENV_NLTK_DATA"] = "{}:/home/artifacts/nltk_data:/opt/nltk_data".format(meta["container_nltk_data"])
    env["APPTAINERENV_MPLCONFIGDIR"] = str(meta["container_matplotlib_config"])
    env["APPTAINERENV_XDG_CACHE_HOME"] = str(meta["container_xdg_cache"])
    if meta.get("container_save_checkpoint"):
        env["APPTAINERENV_PIDSMAKER_SAVE_CHECKPOINT"] = str(meta["container_save_checkpoint"])
    if meta.get("container_resume_checkpoint"):
        env["APPTAINERENV_PIDSMAKER_RESUME_CHECKPOINT"] = str(meta["container_resume_checkpoint"])
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
        env=env,
    )
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

    checkpoint_saved = False
    checkpoint_error = None
    if exit_code == 0 and meta.get("persist_checkpoint"):
        host_checkpoint = Path(str(meta["host_save_checkpoint"]))
        persist_checkpoint = Path(str(meta["persist_checkpoint"]))
        if host_checkpoint.exists():
            persist_checkpoint.parent.mkdir(parents=True, exist_ok=True)
            tmp_checkpoint = persist_checkpoint.with_name(f".{persist_checkpoint.name}.tmp.{os.getpid()}")
            shutil.copy2(host_checkpoint, tmp_checkpoint)
            os.replace(str(tmp_checkpoint), str(persist_checkpoint))
            checkpoint_saved = True
        else:
            checkpoint_error = f"PIDSMaker did not write checkpoint: {host_checkpoint}"
            exit_code = 1

    row = {**meta, "exit_code": exit_code, "duration_sec": round(time.time() - started, 1)}
    row.update(parse_metrics("".join(lines)))
    row["oom"] = bool(row.get("oom")) or exit_code == 137
    if meta.get("persist_checkpoint"):
        row["checkpoint_saved"] = checkpoint_saved
        row["checkpoint_path"] = meta.get("persist_checkpoint")
    if meta.get("resume_checkpoint"):
        row["resume_checkpoint_path"] = meta.get("resume_checkpoint")
    if checkpoint_error:
        row["checkpoint_error"] = checkpoint_error

    results_dir.mkdir(parents=True, exist_ok=True)
    result_path = results_dir / "{}_{}.json".format(meta["phase"], meta["name"])
    tmp_path = result_path.with_name(".{}.tmp.{}".format(result_path.name, os.getpid()))
    tmp_path.write_text(json.dumps(row, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(str(tmp_path), str(result_path))
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sweep", type=Path, required=True)
    parser.add_argument("--index", type=int)
    parser.add_argument("--run-name")
    parser.add_argument("--tag")
    parser.add_argument("--phase", default="run")
    parser.add_argument("--extra-arg", action="append", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sweep = load_sweep(args.sweep.resolve(), args.tag)
    run = select_run(sweep, args)
    command, meta = build_command(sweep, run, args)
    results_dir = Path(str(os.environ.get("MELUXINA_PIDSMAKER_RESULTS_DIR") or sweep["results_dir"])).resolve()
    sys.exit(run_command(command, meta, results_dir))


if __name__ == "__main__":
    main()
