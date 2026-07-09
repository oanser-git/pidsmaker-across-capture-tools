#!/usr/bin/env python3
"""Submit a MeluXina Slurm job from a small YAML file."""

from __future__ import annotations

import argparse
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any

import yaml


SBATCH_FLAGS = {
    "account": "--account",
    "partition": "--partition",
    "qos": "--qos",
    "cpus_per_task": "--cpus-per-task",
    "mem": "--mem",
    "time": "--time",
    "output": "--output",
    "error": "--error",
    "job_name": "--job-name",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    if not isinstance(config, dict):
        raise SystemExit(f"Config must be a YAML mapping: {path}")
    return config


def expand(value: Any, root: Path, config_dir: Path) -> Any:
    if isinstance(value, str):
        return value.replace("${P_EDR_ROOT}", str(root)).replace("${CONFIG_DIR}", str(config_dir))
    if isinstance(value, list):
        return [expand(item, root, config_dir) for item in value]
    if isinstance(value, dict):
        return {key: expand(item, root, config_dir) for key, item in value.items()}
    return value


def resolve_path(value: str, root: Path) -> str:
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str(root / path)


def build_command(config: dict[str, Any], config_path: Path) -> list[str]:
    root = repo_root()
    config_dir = config_path.resolve().parent
    job = expand(config.get("job") or {}, root, config_dir)
    env = expand(config.get("env") or {}, root, config_dir)
    if not isinstance(job, dict):
        raise SystemExit("job must be a mapping")
    if not isinstance(env, dict):
        raise SystemExit("env must be a mapping")

    script = job.get("script")
    if not script:
        raise SystemExit("job.script is required")

    command = ["sbatch", f"--chdir={root}"]
    for key, flag in SBATCH_FLAGS.items():
        value = job.get(key)
        if value not in (None, ""):
            if key in {"output", "error"}:
                value = resolve_path(str(value), root)
            command.append(f"{flag}={value}")

    export_env = {"P_EDR_ROOT": str(root)}
    export_env.update({str(key): str(value) for key, value in env.items()})
    export_arg = "ALL," + ",".join(f"{key}={value}" for key, value in export_env.items())
    command.append(f"--export={export_arg}")

    extra_args = job.get("extra_args") or []
    if isinstance(extra_args, str):
        extra_args = [extra_args]
    command.extend(str(item) for item in extra_args)
    command.append(resolve_path(str(script), root))
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = args.config.resolve()
    command = build_command(load_config(config_path), config_path)
    print("$ " + " ".join(shlex.quote(part) for part in command), flush=True)
    if args.dry_run:
        return
    completed = subprocess.run(command, check=False)
    sys.exit(completed.returncode)


if __name__ == "__main__":
    main()
